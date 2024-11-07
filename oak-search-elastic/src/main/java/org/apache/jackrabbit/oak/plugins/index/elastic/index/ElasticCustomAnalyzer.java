/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import co.elastic.clients.elasticsearch._types.analysis.Analyzer;
import co.elastic.clients.elasticsearch._types.analysis.CharFilterDefinition;
import co.elastic.clients.elasticsearch._types.analysis.CustomAnalyzer;
import co.elastic.clients.elasticsearch._types.analysis.TokenFilterDefinition;
import co.elastic.clients.elasticsearch._types.analysis.TokenizerDefinition;
import co.elastic.clients.elasticsearch.indices.IndexSettingsAnalysis;
import co.elastic.clients.json.JsonData;
import org.apache.jackrabbit.guava.common.base.CaseFormat;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticCustomAnalyzerMappings.ContentTransformer;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticCustomAnalyzerMappings.ParameterTransformer;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.lucene.analysis.AbstractAnalysisFactory;
import org.apache.lucene.analysis.CharFilterFactory;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.en.AbstractWordsFileFilterFactory;
import org.apache.lucene.util.ResourceLoader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticCustomAnalyzerMappings.CONTENT_TRANSFORMERS;
import static org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticCustomAnalyzerMappings.UNSUPPORTED_LUCENE_PARAMETERS;
import static org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticCustomAnalyzerMappings.LUCENE_ELASTIC_TRANSFORMERS;
import static org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticCustomAnalyzerMappings.FILTERS;

/**
 * Loads custom analysis index settings from a JCR NodeState. It also takes care of required transformations from Lucene
 * to Elasticsearch configuration options.
 */
public class ElasticCustomAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticCustomAnalyzer.class);

    private static final String ANALYZER_TYPE = "type";

    private static final Set<String> IGNORE_PROP_NAMES = Set.of(
            AbstractAnalysisFactory.LUCENE_MATCH_VERSION_PARAM,
            FulltextIndexConstants.ANL_CLASS,
            FulltextIndexConstants.ANL_NAME,
            JcrConstants.JCR_PRIMARYTYPE
    );

    private static final ContentTransformer NOOP_TRANSFORMATION = s -> s;

    @Nullable
    public static IndexSettingsAnalysis.Builder buildCustomAnalyzers(NodeState state, String analyzerName) {
        if (state != null) {
            NodeState defaultAnalyzer = state.getChildNode(FulltextIndexConstants.ANL_DEFAULT);
            if (defaultAnalyzer.exists()) {
                IndexSettingsAnalysis.Builder builder = new IndexSettingsAnalysis.Builder();
                Map<String, Object> analyzer = convertNodeState(defaultAnalyzer);
                String builtIn = defaultAnalyzer.getString(FulltextIndexConstants.ANL_CLASS);
                if (builtIn == null) {
                    builtIn = defaultAnalyzer.getString(FulltextIndexConstants.ANL_NAME);
                }
                if (builtIn != null) {
                    analyzer.put(ANALYZER_TYPE, normalize(builtIn));

                    // content params, usually stop words
                    for (ChildNodeEntry nodeEntry : defaultAnalyzer.getChildNodeEntries()) {
                        try {
                            analyzer.put(normalize(nodeEntry.getName()), loadContent(nodeEntry.getNodeState(), nodeEntry.getName(), NOOP_TRANSFORMATION));
                        } catch (IOException e) {
                            throw new IllegalStateException("Unable to load content for node entry " + nodeEntry.getName(), e);
                        }
                    }

                    builder.analyzer(analyzerName, new Analyzer(null, JsonData.of(analyzer)));
                } else { // try to compose the analyzer
                    builder.tokenizer("custom_tokenizer", tb ->
                            tb.definition(loadTokenizer(defaultAnalyzer.getChildNode(FulltextIndexConstants.ANL_TOKENIZER))));

                    LinkedHashMap<String, TokenFilterDefinition> tokenFilters = loadFilters(
                            defaultAnalyzer.getChildNode(FulltextIndexConstants.ANL_FILTERS),
                            TokenFilterFactory::lookupClass, TokenFilterDefinition::new
                    );
                    tokenFilters.forEach((key, value) -> builder.filter(key, fn -> fn.definition(value)));

                    LinkedHashMap<String, CharFilterDefinition> charFilters = loadFilters(
                            defaultAnalyzer.getChildNode(FulltextIndexConstants.ANL_CHAR_FILTERS),
                            CharFilterFactory::lookupClass, CharFilterDefinition::new
                    );
                    charFilters.forEach((key, value) -> builder.charFilter(key, fn -> fn.definition(value)));

                    builder.analyzer(analyzerName, bf -> bf.custom(CustomAnalyzer.of(cab ->
                            cab.tokenizer("custom_tokenizer")
                                    .filter(List.copyOf(tokenFilters.keySet()))
                                    .charFilter(List.copyOf(charFilters.keySet()))
                    )));
                }
                return builder;
            }
        }
        return null;
    }

    @NotNull
    private static TokenizerDefinition loadTokenizer(NodeState state) {
        String name = normalize(Objects.requireNonNull(state.getString(FulltextIndexConstants.ANL_NAME)));
        Map<String, Object> args = convertNodeState(state);
        args.put(ANALYZER_TYPE, name);
        return new TokenizerDefinition(name, JsonData.of(args));
    }

    private static <FD> LinkedHashMap<String, FD> loadFilters(NodeState state,
                                                              Function<String, Class<? extends AbstractAnalysisFactory>> lookup,
                                                              BiFunction<String, JsonData, FD> factory) {
        LinkedHashMap<String, FD> filters = new LinkedHashMap<>();
        int i = 0;
        //Need to read children in order
        Tree tree = TreeFactory.createReadOnlyTree(state);
        for (Tree t : tree.getChildren()) {
            NodeState child = state.getChildNode(t.getName());

            String name;
            List<String> content = null;
            List<ParameterTransformer> transformers;
            try {
                Class<? extends AbstractAnalysisFactory> tff = lookup.apply(t.getName());

                List<String> unsupportedParameters =
                        UNSUPPORTED_LUCENE_PARAMETERS.entrySet().stream()
                                .filter(k -> k.getKey().isAssignableFrom(tff))
                                .map(Map.Entry::getValue)
                                .findFirst().orElseGet(Collections::emptyList);
                Map<String, String> luceneArgs = StreamSupport.stream(child.getProperties().spliterator(), false)
                        .filter(ElasticCustomAnalyzer::isPropertySupported)
                        .filter(ps -> !unsupportedParameters.contains(ps.getName()))
                        .collect(Collectors.toMap(PropertyState::getName, ps -> ps.getValue(Type.STRING)));

                AbstractAnalysisFactory luceneFactory = tff.getConstructor(Map.class).newInstance(luceneArgs);
                if (luceneFactory instanceof AbstractWordsFileFilterFactory) {
                    AbstractWordsFileFilterFactory wordsFF = ((AbstractWordsFileFilterFactory) luceneFactory);
                    // this will parse/load the content handling different formats, comments, etc
                    wordsFF.inform(new NodeStateResourceLoader(child));
                    content = wordsFF.getWords().stream().map(w -> new String(((char[]) w))).collect(Collectors.toList());
                }

                name = normalize((String) tff.getField("NAME").get(null));
                transformers = LUCENE_ELASTIC_TRANSFORMERS.entrySet().stream()
                        .filter(k -> k.getKey().isAssignableFrom(tff))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
            } catch (Exception e) {
                LOG.warn("Unable to introspect Lucene internal factories for transformations. " +
                        "If using an Elasticsearch-specific factory, consider using a Lucene-compatible one for backward compatibility. " +
                        "Current configuration will be used. Error: {}", e.getMessage());
                LOG.debug("Error details: ", e);
                name = normalize(t.getName());
                transformers = List.of();
            }

            Map<String, Object> args = convertNodeState(child, transformers, content);

            // stemmer in elastic don't have language based configurations. They all stay under the stemmer config with
            // a language parameter
            if (name.endsWith("_stem")) {
                String language = FILTERS.get(name);
                if (language == null) {
                    language = name.substring(0, name.length() - "_stem".length());
                    // we then have to reverse the named parts
                    List<String> languageParts = Arrays.asList(language.split("_"));
                    Collections.reverse(languageParts);
                    language = String.join("_", languageParts);
                }
                args.put("language", language);
                name = "stemmer";
            } else {
                if (FILTERS.get(name) != null) {
                    name = FILTERS.get(name);
                }
            }
            args.put(ANALYZER_TYPE, name);

            filters.put(name + "_" + i, factory.apply(name, JsonData.of(args)));
            i++;
        }
        return filters;
    }

    private static List<String> loadContent(NodeState file, String name, ContentTransformer transformer) throws IOException {
        Blob blob = ConfigUtil.getBlob(file, name);
        try (Reader content = new InputStreamReader(Objects.requireNonNull(blob).getNewStream(), StandardCharsets.UTF_8)) {
            try (BufferedReader br = new BufferedReader(content)) {
                return br.lines()
                        .map(String::trim)
                        // apply specific transformations
                        .map(transformer::transform)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            }
        }
    }

    /**
     * Normalizes one of the following values:
     * - lucene class (eg: org.apache.lucene.analysis.en.EnglishAnalyzer -> english)
     * - lucene name (eg: Standard -> standard)
     * into the elasticsearch compatible value
     */
    private static String normalize(String value) {
        // this might be a full class, let's tokenize the value
        String[] anlClassTokens = value.split("\\.");
        if (anlClassTokens.length == 0) {
            throw new IllegalStateException("Cannot extract tokens from value " + value);
        }
        // and take the last part
        String name = anlClassTokens[anlClassTokens.length - 1];
        // all options in elastic are in snake case
        name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
        // if it ends with analyzer we need to get rid of it
        if (name.endsWith("_analyzer")) {
            name = name.substring(0, name.length() - "_analyzer".length());
        }
        return name;
    }

    private static Map<String, Object> convertNodeState(NodeState state) {
        return convertNodeState(state, List.of(), List.of());
    }

    private static Map<String, Object> convertNodeState(NodeState state, List<ParameterTransformer> transformers, List<String> preloadedContent) {
        Map<String, Object> luceneParams = StreamSupport.stream(Spliterators.spliteratorUnknownSize(state.getProperties().iterator(), Spliterator.ORDERED), false)
                .filter(ElasticCustomAnalyzer::isPropertySupported)
                .collect(Collectors.toMap(PropertyState::getName, ps -> {
                    String value = ps.getValue(Type.STRING);
                    List<String> values = Arrays.asList(value.split(","));
                    if (values.stream().allMatch(v -> state.hasChildNode(v.trim()))) {
                        return Objects.requireNonNullElseGet(preloadedContent, () -> values.stream().flatMap(v -> {
                            try {
                                return loadContent(state.getChildNode(v.trim()), v.trim(),
                                        CONTENT_TRANSFORMERS.getOrDefault(ps.getName(), NOOP_TRANSFORMATION)).stream();
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        }).collect(Collectors.toList()));
                    } else return value;
                }));
        return transformers.stream().reduce(luceneParams, (lp, t) -> t.transform(lp), (lp1, lp2) -> {
            lp1.putAll(lp2);
            return lp1;
        });
    }

    /*
     * See org.apache.jackrabbit.oak.plugins.index.lucene.NodeStateAnalyzerFactory#convertNodeState
     */
    private static boolean isPropertySupported(PropertyState ps) {
        return ps.getType() != Type.BINARY &&
                !ps.isArray() &&
                !NodeStateUtils.isHidden(ps.getName()) &&
                !IGNORE_PROP_NAMES.contains(ps.getName());
    }

    /**
     * This loader is just used to load resources in order to benefit from parser (eg: to remove comments or support multiple
     * formats) already implemented in lucene.
     */
    private static class NodeStateResourceLoader implements ResourceLoader {

        private final NodeState nodeState;

        public NodeStateResourceLoader(NodeState nodeState) {
            this.nodeState = nodeState;
        }

        @Override
        public InputStream openResource(String resource) {
            return ConfigUtil.getBlob(nodeState.getChildNode(resource), resource).getNewStream();
        }

        @Override
        public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
            return null;
        }

        @Override
        public <T> T newInstance(String cname, Class<T> expectedType) {
            return null;
        }
    }
}
