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

import org.apache.jackrabbit.guava.common.base.CaseFormat;
import org.apache.lucene.analysis.AbstractAnalysisFactory;
import org.apache.lucene.analysis.charfilter.MappingCharFilterFactory;
import org.apache.lucene.analysis.cjk.CJKBigramFilterFactory;
import org.apache.lucene.analysis.commongrams.CommonGramsFilterFactory;
import org.apache.lucene.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.apache.lucene.analysis.core.TypeTokenFilterFactory;
import org.apache.lucene.analysis.en.AbstractWordsFileFilterFactory;
import org.apache.lucene.analysis.miscellaneous.KeepWordFilterFactory;
import org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilterFactory;
import org.apache.lucene.analysis.miscellaneous.LengthFilterFactory;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory;
import org.apache.lucene.analysis.ngram.EdgeNGramFilterFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.pattern.PatternCaptureGroupFilterFactory;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.util.ElisionFilterFactory;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ElasticCustomAnalyzerMappings {

    /*
     * List of lucene parameters not support in the version referenced in Elastic
     */
    protected static final Map<Class<? extends AbstractAnalysisFactory>, List<String>> UNSUPPORTED_LUCENE_PARAMETERS;

    static {
        UNSUPPORTED_LUCENE_PARAMETERS = Map.of(
                CommonGramsFilterFactory.class, List.of("query_mode"),
                AbstractWordsFileFilterFactory.class, List.of("enablePositionIncrements"),
                SynonymFilterFactory.class, List.of("lenient"),
                EdgeNGramFilterFactory.class, List.of("preserveOriginal"),
                LengthFilterFactory.class, List.of("enablePositionIncrements"),
                NGramFilterFactory.class, List.of("keepShortTerm", "preserveOriginal")
        );
    }

    @FunctionalInterface
    protected interface ContentTransformer {
        @Nullable
        String transform(String line);
    }

    /*
     * In some cases lucene cannot be used for parsing. This map contains pluggable transformers to execute on specific
     * filter keys.
     */
    protected static final Map<String, ContentTransformer> CONTENT_TRANSFORMERS;

    static {
        CONTENT_TRANSFORMERS = new LinkedHashMap<>();
        CONTENT_TRANSFORMERS.put("mapping", line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return null;
            } else {
                return line.replaceAll("\"", "");
            }
        });
    }

    @FunctionalInterface
    protected interface ParameterTransformer {
        /**
         * Transforms the input lucene parameters into elastic ones
         */
        Map<String, Object> transform(Map<String, Object> luceneParams);
    }

    /*
     * Transform function for lucene parameters to elastic
     */
    protected static final Map<Class<? extends AbstractAnalysisFactory>, ParameterTransformer> LUCENE_ELASTIC_TRANSFORMERS;

    static {
        // BiFunction<T, U, R>
        // renames the key from input parameters (T) using the key value map (U) and returns back the transformed parameters (R)
        BiFunction<Map<String, Object>, Map<String, String>, Map<String, Object>> reKey = (luceneParams, keys) -> {
            keys.forEach((key, value) -> {
                if (luceneParams.containsKey(key)) {
                    luceneParams.put(value, luceneParams.remove(key));
                }
            });
            return luceneParams;
        };

        LUCENE_ELASTIC_TRANSFORMERS = new LinkedHashMap<>();

        LUCENE_ELASTIC_TRANSFORMERS.put(WordDelimiterFilterFactory.class, luceneParams -> {
            Consumer<String> transformFlag = flag -> luceneParams.computeIfPresent(flag, (k, v) -> Integer.parseInt(v.toString()) == 1);

            transformFlag.accept("generateWordParts");
            transformFlag.accept("generateNumberParts");
            transformFlag.accept("catenateWords");
            transformFlag.accept("catenateNumbers");
            transformFlag.accept("catenateAll");
            transformFlag.accept("splitOnCaseChange");
            transformFlag.accept("preserveOriginal");
            transformFlag.accept("splitOnNumerics");
            transformFlag.accept("stemEnglishPossessive");

            return reKey.apply(luceneParams, Map.of(
                    "protectedTokens", "protected_words"
            ));
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(TypeTokenFilterFactory.class, luceneParams -> {
            Object useWhitelist = luceneParams.remove("useWhitelist");
            if (useWhitelist != null && Boolean.parseBoolean(useWhitelist.toString())) {
                luceneParams.put("mode", "include");
            } else {
                luceneParams.put("mode", "exclude");
            }

            return luceneParams;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(PatternCaptureGroupFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("pattern", "patterns"))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(KeepWordFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of(
                        "words", "keep_words",
                        "ignoreCase", "keep_words_case"
                ))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(ElisionFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("ignoreCase", "articles_case"))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(EdgeNGramFilterFactory.class, luceneParams -> {
            luceneParams.remove("side");
            return reKey.apply(luceneParams, Map.of(
                    "minGramSize", "min_gram",
                    "maxGramSize", "max_gram"
            ));
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(NGramFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of(
                "minGramSize", "min_gram",
                "maxGramSize", "max_gram"
                ))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(DelimitedPayloadTokenFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("encoder", "encoding"))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(CommonGramsFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("words", "common_words"))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(MappingCharFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("mapping", "mappings"))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(SynonymFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("tokenizerFactory", "tokenizer"))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(KeywordMarkerFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("protected", "keywords"))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(CJKBigramFilterFactory.class, luceneParams -> {
            List<String> ignored = new ArrayList<>();
            if (!Boolean.parseBoolean(luceneParams.getOrDefault("hal", true).toString())) {
                ignored.add("hal");
            }
            if (!Boolean.parseBoolean(luceneParams.getOrDefault("hangul", true).toString())) {
                ignored.add("hangul");
            }
            if (!Boolean.parseBoolean(luceneParams.getOrDefault("hiragana", true).toString())) {
                ignored.add("hiragana");
            }
            if (!Boolean.parseBoolean(luceneParams.getOrDefault("katakana", true).toString())) {
                ignored.add("katakana");
            }
            if (!ignored.isEmpty()) {
                luceneParams.put("ignored_scripts", ignored);
            }
            return luceneParams;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(AbstractWordsFileFilterFactory.class, luceneParams -> {
            luceneParams.remove("enablePositionIncrements");
            return reKey.apply(luceneParams, Map.of("words", "stopwords"));
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(DictionaryCompoundWordTokenFilterFactory.class, luceneParams -> reKey.apply(luceneParams, Map.of(
                "dictionary", "word_list"
        )));

        // default transformer executed as final step on all the filters to transform the keys from camel case to snake case
        LUCENE_ELASTIC_TRANSFORMERS.put(AbstractAnalysisFactory.class, luceneParams ->
                luceneParams.entrySet().stream()
                        .collect(Collectors.toMap(
                                entry -> CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey()),
                                Map.Entry::getValue, // keep the original value
                                (oldValue, newValue) -> oldValue, // in case of duplicate keys, keep the old value
                                LinkedHashMap::new // preserve the original order
                        ))
        );
    }

    /*
     * Some filter names cannot be transformed from the original name. Here we map the exceptions
     */
    protected static final Map<String, String> FILTERS = Map.ofEntries(
            Map.entry("porter_stem", "porter2"),
            Map.entry("ascii_folding", "asciifolding"),
            Map.entry("n_gram", "ngram"),
            Map.entry("edge_n_gram", "edge_ngram"),
            Map.entry("keep_word", "keep"),
            Map.entry("k_stem", "kstem"),
            Map.entry("limit_token_count", "limit"),
            Map.entry("pattern_capture_group", "pattern_capture"),
            Map.entry("reverse_string", "reverse"),
            Map.entry("snowball_porter", "snowball"),
            Map.entry("dictionary_compound_word", "dictionary_decompounder"),
            Map.entry("type", "keep_types")
    );
}
