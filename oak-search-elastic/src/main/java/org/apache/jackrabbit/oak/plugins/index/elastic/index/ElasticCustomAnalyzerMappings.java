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
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilterFactory;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ElasticCustomAnalyzerMappings {

    /*
     * Compatibility mappings between the different lucene versions
     */
    protected static final Map<Class<? extends AbstractAnalysisFactory>, Map<String, String>> LUCENE_VERSIONS;

    static {

        LUCENE_VERSIONS = new LinkedHashMap<>();
        LUCENE_VERSIONS.put(CommonGramsFilterFactory.class, Map.of("query_mode", ""));
        LUCENE_VERSIONS.put(AbstractWordsFileFilterFactory.class, Map.of("enablePositionIncrements", ""));
        LUCENE_VERSIONS.put(SynonymFilterFactory.class, Map.of("lenient", ""));
    }

    /*
     * Transform function for lucene parameters to elastic
     */
    protected static final Map<Class<? extends AbstractAnalysisFactory>, Function<Map<String, Object>, Map<String, Object>>> LUCENE_ELASTIC_TRANSFORMERS;

    static {
        LUCENE_ELASTIC_TRANSFORMERS = new LinkedHashMap<>();

        LUCENE_ELASTIC_TRANSFORMERS.put(DelimitedPayloadTokenFilterFactory.class, luceneParams -> {
            if (luceneParams.containsKey("encoder")) {
                luceneParams.put("encoding", luceneParams.remove("encoder"));
            }
            return luceneParams;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(CommonGramsFilterFactory.class, luceneParams -> {
            if (luceneParams.containsKey("words")) {
                luceneParams.put("common_words", luceneParams.remove("words"));
            }
            return luceneParams;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(MappingCharFilterFactory.class, luceneParams -> {
            if (luceneParams.containsKey("mapping")) {
                luceneParams.put("mappings", luceneParams.remove("mapping"));
            }
            return luceneParams;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(SynonymFilterFactory.class, luceneParams -> {
            if (luceneParams.containsKey("tokenizerFactory")) {
                luceneParams.put("tokenizer", luceneParams.remove("tokenizerFactory"));
            }
            return luceneParams;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(KeywordMarkerFilterFactory.class, luceneParams -> {
            if (luceneParams.containsKey("protected")) {
                luceneParams.put("keywords", luceneParams.remove("protected"));
            }
            return luceneParams;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(ASCIIFoldingFilterFactory.class, luceneParams -> {
            if (luceneParams.containsKey("preserveOriginal")) {
                luceneParams.put("preserve_original", luceneParams.remove("preserveOriginal"));
            }
            return luceneParams;
        });

        LUCENE_ELASTIC_TRANSFORMERS.put(CJKBigramFilterFactory.class, luceneParams -> {
            if (luceneParams.containsKey("outputUnigrams")) {
                luceneParams.put("output_unigrams", luceneParams.remove("outputUnigrams"));
            }
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
            if (luceneParams.containsKey("words")) {
                luceneParams.put("stopwords", luceneParams.remove("words"));
            }
            if (luceneParams.containsKey("ignoreCase")) {
                luceneParams.put("ignore_case", luceneParams.remove("ignoreCase"));
            }
            luceneParams.remove("enablePositionIncrements");
            return luceneParams;
        });
    }

    /*
     * Some filter names cannot be transformed from the original name. Here we map the exceptions
     */
    protected static final Map<String, String> FILTERS = Map.of(
            "porter_stem", "porter2",
            "ascii_folding", "asciifolding"
    );
}
