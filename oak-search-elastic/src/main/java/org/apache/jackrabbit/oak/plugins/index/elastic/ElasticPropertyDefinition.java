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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

public class ElasticPropertyDefinition extends PropertyDefinition {

    SimilaritySearchParameters similaritySearchParameters;

    public static final String PROP_QUERY_MODEL = "queryModel";
    public static final String PROP_NUMBER_OF_HASH_TABLES = "L";
    public static final String PROP_NUMBER_OF_HASH_FUNCTIONS = "k";
    public static final String PROP_NUMBER_OF_BUCKETS = "w";
    public static final String PROP_INDEX_SIMILARITY = "indexSimilarity";
    public static final String PROP_QUERY_SIMILARITY = "querySimilarity";
    public static final String PROP_CANDIDATES = "candidates";
    public static final String PROP_PROBES = "probes";

    private static final int DEFAULT_NUMBER_OF_HASH_TABLES = 20;
    private static final int DEFAULT_NO_OF_HASH_FUNCTIONS = 15;
    private static final int DEFAULT_BUCKET_WIDTH = 500;
    private static final String DEFAULT_SIMILARITY_QUERY_MODEL = "lsh";
    private static final String DEFAULT_SIMILARITY_INDEX_FUNCTION = "l2";
    private static final String DEFAULT_SIMILARITY_QUERY_FUNCTION = "l2";
    private static final int DEFAULT_QUERY_CANDIDATES = 500;
    private static final int DEFAULT_QUERY_PROBES = 3;


    public ElasticPropertyDefinition(IndexDefinition.IndexingRule idxDefn, String nodeName, NodeState defn) {
        super(idxDefn, nodeName, defn);
        if (this.useInSimilarity) {
            similaritySearchParameters = new SimilaritySearchParameters(
                    getOptionalValue(defn, PROP_NUMBER_OF_HASH_TABLES, DEFAULT_NUMBER_OF_HASH_TABLES),
                    getOptionalValue(defn, PROP_NUMBER_OF_HASH_FUNCTIONS, DEFAULT_NO_OF_HASH_FUNCTIONS),
                    getOptionalValue(defn, PROP_NUMBER_OF_BUCKETS, DEFAULT_BUCKET_WIDTH),
                    getOptionalValue(defn, PROP_QUERY_MODEL, DEFAULT_SIMILARITY_QUERY_MODEL),
                    getOptionalValue(defn, PROP_INDEX_SIMILARITY, DEFAULT_SIMILARITY_INDEX_FUNCTION),
                    getOptionalValue(defn, PROP_QUERY_SIMILARITY, DEFAULT_SIMILARITY_QUERY_FUNCTION),
                    getOptionalValue(defn, PROP_CANDIDATES, DEFAULT_QUERY_CANDIDATES),
                    getOptionalValue(defn, PROP_PROBES, DEFAULT_QUERY_PROBES));
        }
    }

    /**
     * Class for defining parameters for similarity search based on https://elastiknn.com/api.
     * For all possible models and query combinations, see https://elastiknn.com/api/#model-and-query-compatibility
     */
    public static class SimilaritySearchParameters {

        /**
         * Number of hash tables. Generally, increasing this value increases recall.
         */
        private final int L;
        /**
         * Number of hash functions combined to form a single hash value. Generally, increasing this value increases precision.
         */
        private final int k;
        /**
         * Integer bucket width.
         */
        private final int w;
        /**
         * Possible values - lsh, exact
         */
        private final String queryModel;
        /**
         * Possible values l2 (with lsh or exact model), l1 (with exact model), A (angular distance - with exact model)
         */
        private final String queryTimeSimilarityFunction;
        /**
         * Possible values l2 (with lsh or exact model), l1 (with exact model), A (angular distance - with exact model)
         */
        private final String indexTimeSimilarityFunction;
        /**
         * Take the top vectors with the most matching hashes and compute their exact similarity to the query vector. The candidates parameter
         * controls the number of exact similarity computations. Specifically, we compute exact similarity for the top candidates candidate vectors
         * in each segment. As a reminder, each Elasticsearch index has >= 1 shards, and each shard has >= 1 segments. That means if you set
         * "candidates": 200 for an index with 2 shards, each with 3 segments, then you’ll compute the exact similarity for 2 * 3 * 200 = 1200 vectors.
         * candidates must be set to a number greater or equal to the number of Elasticsearch results you want to get. Higher values generally mean
         * higher recall and higher latency.
         */
        private final int candidates;
        /**
         * Number of probes for using the multiprobe search technique. Default value is zero. Max value is 3^k. Generally, increasing probes will
         * increase recall, will allow you to use a smaller value for L with comparable recall, but introduces some additional computation at query time.
         */
        private final int probes;

        public SimilaritySearchParameters(int l, int k, int w, String queryModel, String indexTimeSimilarityFunction,
                                          String queryTimeSimilarityFunction, int candidates, int probes) {
            L = l;
            this.k = k;
            this.w = w;
            this.queryModel = queryModel;
            this.indexTimeSimilarityFunction = indexTimeSimilarityFunction;
            this.queryTimeSimilarityFunction = queryTimeSimilarityFunction;
            this.candidates = candidates;
            this.probes = probes;
        }

        public int getL() {
            return L;
        }

        public int getK() {
            return k;
        }

        public int getW() {
            return w;
        }

        public String getQueryModel() {
            return queryModel;
        }

        public String getQueryTimeSimilarityFunction() {
            return queryTimeSimilarityFunction;
        }

        public String getIndexTimeSimilarityFunction() {
            return indexTimeSimilarityFunction;
        }

        public int getCandidates() {
            return candidates;
        }

        public int getProbes() {
            return probes;
        }
    }

    public SimilaritySearchParameters getSimilaritySearchParameters() {
        return similaritySearchParameters;
    }
}
