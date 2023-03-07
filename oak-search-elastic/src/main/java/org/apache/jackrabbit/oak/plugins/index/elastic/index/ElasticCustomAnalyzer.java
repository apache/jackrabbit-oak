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
import com.google.common.base.CaseFormat;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.lucene.analysis.charfilter.MappingCharFilterFactory;
import org.apache.lucene.analysis.en.AbstractWordsFileFilterFactory;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
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

    /*
     * Mappings for lucene options not available anymore to supported elastic counterparts
     */
    private static final Map<Class<? extends AbstractAnalysisFactory>, Map<String, String>> CONFIGURATION_MAPPING;

    static {
        CONFIGURATION_MAPPING = new LinkedHashMap<>();
        CONFIGURATION_MAPPING.put(AbstractWordsFileFilterFactory.class, Map.of("words", "stopwords"));
        CONFIGURATION_MAPPING.put(MappingCharFilterFactory.class, Map.of("mapping", "mappings"));
    }

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
                            analyzer.put(normalize(nodeEntry.getName()), loadContent(nodeEntry.getNodeState(), nodeEntry.getName()));
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
            Class<? extends AbstractAnalysisFactory> tff = lookup.apply(t.getName());
            String name;
            try {
                name = normalize((String) tff.getField("NAME").get(null));
            } catch (Exception e) {
                LOG.warn("unable to get the filter name using reflection. Try using the normalized node name", e);
                name = normalize(t.getName());
            }
            Map<String, String> mappings =
                    CONFIGURATION_MAPPING.entrySet().stream()
                            .filter(k -> k.getKey().isAssignableFrom(tff))
                            .map(Map.Entry::getValue)
                            .findFirst().orElseGet(Collections::emptyMap);
            Map<String, Object> args = convertNodeState(child, mappings);

            args.put(ANALYZER_TYPE, name);

            filters.put(name + "_" + i, factory.apply(name, JsonData.of(args)));
            i++;
        }
        return filters;
    }

    private static List<String> loadContent(NodeState file, String name) throws IOException {
        Blob blob = ConfigUtil.getBlob(file, name);
        try (Reader content = new InputStreamReader(Objects.requireNonNull(blob).getNewStream(), StandardCharsets.UTF_8)) {
            try (BufferedReader br = new BufferedReader(content)) {
                return br.lines()
                        .map(String::trim)
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
        return convertNodeState(state, Collections.emptyMap());
    }

    private static Map<String, Object> convertNodeState(NodeState state, Map<String, String> mapping) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(state.getProperties().iterator(), Spliterator.ORDERED), false)
                .filter(ps -> ps.getType() != Type.BINARY &&
                        !ps.isArray() &&
                        !NodeStateUtils.isHidden(ps.getName()) &&
                        !IGNORE_PROP_NAMES.contains(ps.getName())
                ).collect(Collectors.toMap(ps -> {
                    String remappedName = mapping.get(ps.getName());
                    return remappedName != null ? remappedName : ps.getName();
                }, ps -> {
                    String value = ps.getValue(Type.STRING);
                    List<String> values = Arrays.asList(value.split(","));
                    if (values.stream().allMatch(v -> state.hasChildNode(v.trim()))) {
                        return values.stream().flatMap(v -> {
                            try {
                                return loadContent(state.getChildNode(v.trim()), v.trim()).stream();
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        }).collect(Collectors.toList());
                    } else return value;
                }));
    }
}
