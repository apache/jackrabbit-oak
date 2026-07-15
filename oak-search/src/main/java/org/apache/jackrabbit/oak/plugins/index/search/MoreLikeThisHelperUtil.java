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
package org.apache.jackrabbit.oak.plugins.index.search;

import java.util.HashMap;
import java.util.Map;

/*
Helper class to assist with mlt query formation for elastic and lucene
 */
public class MoreLikeThisHelperUtil {

    /*
        A list of fields to fetch and analyze the text from.
        Default analyzes all the indexed fields.
     */
    public static final String MLT_FILED = "mlt.fl";

    /*
        The minimum document frequency for a term below which the terms will be ignored from the input document.
        Defaults to 5
     */
    public static final String MLT_MIN_DOC_FREQ = "mlt.mindf";

    /*
        The maximum document frequency above which the terms will be ignored from the input document.
        This could be useful in order to ignore highly frequent words such as stop words. Defaults to INTEGER.MAX
     */
    public static final String MLT_MAX_DOC_FREQ = "mlt.maxdf";

    /*
        The minimum term frequency (Number of times the term occurs in the input doc)
        below which the terms will be ignored from the input document.
        Defaults to 2
     */
    public static final String MLT_MIN_TERM_FREQ = "mlt.mintf";

    /*
        Bool value if boost should be supported or not. Only valid for lucene
        Not available in elastic.
     */
    public static final String MLT_BOOST = "mlt.boost";

    /*
        Sets the boost value of the whole query. Defaults to 1.0.
     */
    public static final String MLT_BOOST_FACTOR = "mlt.qf";

    /*
        Only For Lucene
     */
    public static final String MLT_MAX_DOC_FREQ_PCT = "mlt.maxdfp";

    /*
        Only For Lucene
     */
    public static final String MLT_MAX_NUM_TOKENS_PARSED = "mlt.maxntp";

    /*
        The maximum number of query terms that will be selected.
        Increasing this value gives greater accuracy at the expense of query execution speed. Defaults to 25.
     */
    public static final String MLT_MAX_QUERY_TERMS = "mlt.maxqt";

    /*
        The maximum word length above which the terms will be ignored. The old name max_word_len is deprecated.
        Defaults to unbounded
     */
    public static final String MLT_MAX_WORD_LENGTH = "mlt.maxwl";

    /*
        The minimum word length below which the terms will be ignored.
        Defaults to 0
     */
    public static final String MLT_MIN_WORD_LENGTH = "mlt.minwl";

    /*
        An array of stop words.
        Any word in this set is considered "uninteresting" and ignored.
        Only applicable for ELASTIC
     */
    public static final String MLT_STOP_WORDS = "mlt.stopwords";

    /*
        This should have either the id to the doc whose similar docs need to be searched or the complete body of the doc.
        Defautls to ID (via the rep:similar query).
     */
    public static final String MLT_STREAM_BODY = "stream.body";

    /*
        After the disjunctive query has been formed,
        this parameter controls the number of terms that must match.
        (Defaults to "30%").
        Only applicable for ELASTIC
     */
    public static final String MLT_MIN_SHOULD_MATCH = "mlt.minshouldmatch";

    /*
    Returns param map for a query string of type mlt.fl=:path&mlt.mindf=0&stream.body=/test/a
     */
    public static Map<String, String> getParamMapFromMltQuery(String mltQueryString) {
        Map<String, String> paramMap = new HashMap<>();
        try {
            for (String param : mltQueryString.split("&")) {
                String[] keyValuePair = param.split("=");
                if (keyValuePair.length != 2 || keyValuePair[0] == null || keyValuePair[1] == null) {
                    throw new RuntimeException("Unparsable native MLT query: " + mltQueryString);
                } else {
                    paramMap.put(keyValuePair[0], keyValuePair[1]);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while parsing native MLT query: " + mltQueryString);
        }

        if (paramMap.size() == 0) {
            throw new RuntimeException("No params found while parsing the MLT query : " + mltQueryString);
        }

        return paramMap;
    }

}
