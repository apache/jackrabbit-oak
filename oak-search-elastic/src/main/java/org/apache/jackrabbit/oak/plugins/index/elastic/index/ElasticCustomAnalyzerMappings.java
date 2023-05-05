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
import org.apache.lucene.analysis.en.AbstractWordsFileFilterFactory;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class ElasticCustomAnalyzerMappings {

    /*
     * Compatibility mappings between the different lucene versions
     */
    protected static final Map<Class<? extends AbstractAnalysisFactory>, Map<String, String>> LUCENE_VERSIONS;

    static {
        LUCENE_VERSIONS = new LinkedHashMap<>();
        LUCENE_VERSIONS.put(AbstractWordsFileFilterFactory.class, Map.of("enablePositionIncrements", ""));
        LUCENE_VERSIONS.put(SynonymFilterFactory.class, Map.of("lenient", ""));
    }

    /*
     * Mappings for lucene options not available anymore to supported elastic counterparts
     */
    protected static final Map<Class<? extends AbstractAnalysisFactory>, Map<String, String>> LUCENE_ELASTIC;

    static {
        LUCENE_ELASTIC = new LinkedHashMap<>();
        LUCENE_ELASTIC.put(AbstractWordsFileFilterFactory.class, Map.of(
                "words", "stopwords",
                "ignoreCase", "ignore_case",
                "enablePositionIncrements", ""
        ));
        LUCENE_ELASTIC.put(MappingCharFilterFactory.class, Map.of("mapping", "mappings"));
        LUCENE_ELASTIC.put(SynonymFilterFactory.class, Map.of("tokenizerFactory", "tokenizer"));
    }

    /*
     * Some stemmer name cannot be transformed from the original name. Here we map the exceptions
     */
    protected static final Map<String, String> STEMMER = Map.of("porter_stem", "porter2");
}
