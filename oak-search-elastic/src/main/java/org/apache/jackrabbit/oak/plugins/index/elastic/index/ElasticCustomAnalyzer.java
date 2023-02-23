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
import co.elastic.clients.elasticsearch._types.analysis.CustomAnalyzer;
import co.elastic.clients.elasticsearch._types.analysis.TokenFilterDefinition;
import co.elastic.clients.elasticsearch._types.analysis.TokenizerDefinition;
import co.elastic.clients.elasticsearch.indices.IndexSettingsAnalysis;
import co.elastic.clients.json.JsonData;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticCustomAnalyzer {

    private static final Set<String> IGNORE_PROP_NAMES = new HashSet<>(
            Arrays.asList(FulltextIndexConstants.ANL_CLASS, FulltextIndexConstants.ANL_NAME, JcrConstants.JCR_PRIMARYTYPE));

    @Nullable
    public static IndexSettingsAnalysis.Builder buildCustomAnalyzers(NodeState state) {
        if (state != null) {
            NodeState defaultAnalyzer = state.getChildNode(FulltextIndexConstants.ANL_DEFAULT);
            if (defaultAnalyzer.exists()) {
                IndexSettingsAnalysis.Builder builder = new IndexSettingsAnalysis.Builder();
                Map<String, Object> analyzer = new HashMap<>();
                String builtIn = defaultAnalyzer.getString(FulltextIndexConstants.ANL_CLASS);
                if (builtIn == null) {
                    builtIn = defaultAnalyzer.getString(FulltextIndexConstants.ANL_NAME);
                }
                if (builtIn != null) {
                    analyzer.put("type", normalize(builtIn));

                    // additional builtin params
                    for (PropertyState ps : defaultAnalyzer.getProperties()) {
                        if (!IGNORE_PROP_NAMES.contains(ps.getName())) {
                            analyzer.put(normalize(ps.getName()), getValue(ps));
                        }
                    }

                    // content params, usually stop words
                    for (ChildNodeEntry nodeEntry : defaultAnalyzer.getChildNodeEntries()) {
                        try {
                            analyzer.put(normalize(nodeEntry.getName()), loadContent(nodeEntry.getNodeState(), nodeEntry.getName()));
                        } catch (IOException e) {
                            throw new IllegalStateException("Unable to load content for node entry " + nodeEntry.getName(), e);
                        }
                    }

                    builder.analyzer("oak_analyzer", new Analyzer(null, JsonData.of(analyzer)));
                } else { // try to compose the analyzer
                    builder.tokenizer("oak_tokenizer", tb -> tb.definition(loadTokenizer(defaultAnalyzer.getChildNode(FulltextIndexConstants.ANL_TOKENIZER))));

                    LinkedHashMap<String, TokenFilterDefinition> filters = loadTokenFilters(defaultAnalyzer.getChildNode(FulltextIndexConstants.ANL_FILTERS));
                    filters.entrySet().forEach(tfd -> builder.filter(tfd.getKey(), fn -> fn.definition(tfd.getValue())));

                    builder.analyzer("oak_analyzer", bf -> {
                        return bf.custom(CustomAnalyzer.of(cab ->
                                cab.tokenizer("oak_tokenizer")
                                        .filter(new ArrayList<>(filters.keySet()))
                        ));
                    });
                }
                return builder;
            }
        }
        return null;
    }

    @NotNull
    private static TokenizerDefinition loadTokenizer(NodeState state) {
        String name = normalize(checkNotNull(state.getString(FulltextIndexConstants.ANL_NAME)));
        Map<String, String> args = convertNodeState(state);
        args.put("type", name);
        return new TokenizerDefinition(name, JsonData.of(args));
    }

    private static LinkedHashMap<String, TokenFilterDefinition> loadTokenFilters(NodeState state) {
        LinkedHashMap<String, TokenFilterDefinition> filters = new LinkedHashMap<>();
        int i = 0;
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            String name = normalize(entry.getName());
            Map<String, String> args = convertNodeState(entry.getNodeState());
            args.put("type", name);

            filters.put("oak_token_filter_" + i++, new TokenFilterDefinition(name, JsonData.of(args)));
        }
        return filters;
    }

    private static List<String> loadContent(NodeState file, String name) throws IOException {
        List<String> result = new ArrayList<>();
        Blob blob = ConfigUtil.getBlob(file, name);
        Reader content = new InputStreamReader(blob.getNewStream(), StandardCharsets.UTF_8);
        try {
            BufferedReader br = null;
            try {
                br = new BufferedReader(content);
                String word;
                while ((word = br.readLine()) != null) {
                    result.add(word.trim());
                }
            } finally {
                IOUtils.close(br);
            }
            return result;
        } finally {
            IOUtils.close(content);
        }
    }

    private static String getValue(PropertyState state) {
        if (state.getType() == Type.BINARY) {

        } else {
            return state.getValue(Type.STRING);
        }
        return null;
    }

    /**
     * Normalizes one of the following values:
     * - lucene class (eg: org.apache.lucene.analysis.en.EnglishAnalyzer)
     * - lucene name (eg: Standard)
     * into the elasticsearch compatible value
     */
    private static String normalize(String value) {
        String[] anlClassTokens = value.split("\\.");
        String name = anlClassTokens[anlClassTokens.length - 1];
        return name.toLowerCase().replace("analyzer", "");
    }

    private static Map<String, String> convertNodeState(NodeState state) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(state.getProperties().iterator(), Spliterator.ORDERED), false)
                .filter(ps -> ps.getType() != Type.BINARY)
                .filter(ps -> !ps.isArray())
                .filter(ps -> !NodeStateUtils.isHidden(ps.getName()))
                .filter(ps -> !IGNORE_PROP_NAMES.contains(ps.getName()))
                .collect(Collectors.toMap(PropertyState::getName, ps -> ps.getValue(Type.STRING)));
    }
}
