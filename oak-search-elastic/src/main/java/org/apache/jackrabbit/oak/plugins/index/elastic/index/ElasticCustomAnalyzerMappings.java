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

import org.apache.lucene.analysis.charfilter.MappingCharFilterFactory;
import org.apache.lucene.analysis.cjk.CJKBigramFilterFactory;
import org.apache.lucene.analysis.commongrams.CommonGramsFilterFactory;
import org.apache.lucene.analysis.en.AbstractWordsFileFilterFactory;
import org.apache.lucene.analysis.minhash.MinHashFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.miscellaneous.KeepWordFilterFactory;
import org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilterFactory;
import org.apache.lucene.analysis.miscellaneous.LengthFilterFactory;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilterFactory;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory;
import org.apache.lucene.analysis.ngram.EdgeNGramFilterFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.pattern.PatternCaptureGroupFilterFactory;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory;
import org.apache.lucene.analysis.shingle.ShingleFilterFactory;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.ElisionFilterFactory;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
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
            if (line.length() == 0 || line.startsWith("#")) {
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
            Map<String, Object> params = reKey.apply(luceneParams, Map.of(
                    "generateWordParts", "generate_word_parts",
                    "generateNumberParts", "generate_number_parts",
                    "catenateWords", "catenate_words",
                    "catenateNumbers", "catenate_numbers",
                    "catenateAll", "catenate_all",
                    "splitOnCaseChange", "split_on_case_change",
                    "preserveOriginal", "preserve_original",
                    "splitOnNumerics", "split_on_numerics",
                    "stemEnglishPossessive", "stem_english_possessive",
                    "protectedTokens", "protected_words"
            ));
            if (params.containsKey("generate_word_parts")) {
                params.put("generate_word_parts", Integer.parseInt(params.get("generate_word_parts").toString()) == 1);
            }
            if (params.containsKey("generate_number_parts")) {
                params.put("generate_number_parts", Integer.parseInt(params.get("generate_number_parts").toString()) == 1);
            }
            if (params.containsKey("catenate_words")) {
                params.put("catenate_words", Integer.parseInt(params.get("catenate_words").toString()) == 1);
            }
            if (params.containsKey("catenate_numbers")) {
                params.put("catenate_numbers", Integer.parseInt(params.get("catenate_numbers").toString()) == 1);
            }
            if (params.containsKey("catenate_all")) {
                params.put("catenate_all", Integer.parseInt(params.get("catenate_all").toString()) == 1);
            }
            if (params.containsKey("split_on_case_change")) {
                params.put("split_on_case_change", Integer.parseInt(params.get("split_on_case_change").toString()) == 1);
            }
            if (params.containsKey("preserve_original")) {
                params.put("preserve_original", Integer.parseInt(params.get("preserve_original").toString()) == 1);
            }
            if (params.containsKey("split_on_numerics")) {
                params.put("split_on_numerics", Integer.parseInt(params.get("split_on_numerics").toString()) == 1);
            }
            if (params.containsKey("stem_english_possessive")) {
                params.put("stem_english_possessive", Integer.parseInt(params.get("stem_english_possessive").toString()) == 1);
            }
            return params;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(ShingleFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of(
                        "minShingleSize", "min_shingle_size",
                        "maxShingleSize", "max_shingle_size",
                        "outputUnigrams", "output_unigrams",
                        "outputUnigramsIfNoShingles", "output_unigrams_if_no_shingles",
                        "tokenSeparator", "token_separator",
                        "fillerToken", "filler_token"
                ))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(PatternCaptureGroupFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("pattern", "patterns"))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(MinHashFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of(
                        "hashCount", "hash_count",
                        "bucketCount", "bucket_count",
                        "hashSetSize", "hash_set_size",
                        "withRotation", "with_rotation"
                ))
        );

        LUCENE_ELASTIC_TRANSFORMERS.put(LimitTokenCountFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of(
                        "maxTokenCount", "max_token_count",
                        "consumeAllTokens", "consume_all_tokens"
                ))
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

        LUCENE_ELASTIC_TRANSFORMERS.put(ASCIIFoldingFilterFactory.class, luceneParams ->
                reKey.apply(luceneParams, Map.of("preserveOriginal", "preserve_original"))
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
            return reKey.apply(luceneParams, Map.of("outputUnigrams", "output_unigrams"));
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(AbstractWordsFileFilterFactory.class, luceneParams -> {
            luceneParams.remove("enablePositionIncrements");
            return reKey.apply(luceneParams, Map.of("words", "stopwords", "ignoreCase", "ignore_case"));
        });
    }

    /*
     * Some filter names cannot be transformed from the original name. Here we map the exceptions
     */
    protected static final Map<String, String> FILTERS = Map.of(
            "porter_stem", "porter2",
            "ascii_folding", "asciifolding",
            "n_gram", "ngram",
            "edge_n_gram", "edge_ngram",
            "keep_word", "keep",
            "k_stem", "kstem",
            "limit_token_count", "limit",
            "pattern_capture_group", "pattern_capture",
            "reverse_string", "reverse",
            "snowball_porter", "snowball"
    );
}
