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

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ElasticPropertyDefinition extends PropertyDefinition {

  public static final String DEFAULT_SIMILARITY_METRIC = "l2_norm";
  static final String PROP_SIMILARITY_METRIC = "similarityMetric";
  private static final String PROP_SIMILARITY = "similarity";
  private static final String PROP_K = "k";
  private static final String PROP_CANDIDATES = "candidates";
  private static final float DEFAULT_SIMILARITY = 0.95f;
  private static final int DEFAULT_K = 10;
  private static final int DEFAULT_CANDIDATES = 500;
  private KnnSearchParameters knnSearchParameters;

  public ElasticPropertyDefinition(IndexDefinition.IndexingRule idxDefn, String nodeName, NodeState defn) {
    super(idxDefn, nodeName, defn);
    if (this.useInSimilarity) {
      knnSearchParameters = new KnnSearchParameters(
          getOptionalValue(defn, PROP_SIMILARITY_METRIC, DEFAULT_SIMILARITY_METRIC),
          getOptionalValue(defn, PROP_SIMILARITY, DEFAULT_SIMILARITY),
          getOptionalValue(defn, PROP_K, DEFAULT_K),
          getOptionalValue(defn, PROP_CANDIDATES, DEFAULT_CANDIDATES));
    }
  }

  public KnnSearchParameters getKnnSearchParameters() {
    return knnSearchParameters;
  }

  /**
   * Class for defining parameters of approximate knn search on dense_vector fields
   * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html">...</a> and
   * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html">...</a>
   */
  public static class KnnSearchParameters {

    public KnnSearchParameters(String similarityMetric, float similarity, int k, int candidates) {
      this.similarityMetric = similarityMetric;
      this.similarity = similarity;
      this.k = k;
      this.candidates = candidates;
    }

    /**
     * Similarity metric used to compare query and document vectors. Possible values are l2_norm (default), cosine,
     * dot_product, max_inner_product
     */
    private final String similarityMetric;
    /**
     * Minimum similarity for the document vector to be considered as a match. Required when cosine, dot_product
     * or max_inner_product is set as similarityMetric
     */
    private final float similarity;
    /**
     * Number of nearest neighbours to return. Must be <= candidates
     * vector added as a field
     */
    private final int k;

    /**
     * Take the top vectors with the most matching hashes and compute their exact similarity to the query vector. The
     * candidates parameter controls the number of exact similarity computations. Specifically, we compute exact
     * similarity for the top candidates candidate vectors in each segment. As a reminder, each Elasticsearch index has
     * >= 1 shards, and each shard has >= 1 segments. That means if you set "candidates": 200 for an index with 2
     * shards, each with 3 segments, then you’ll compute the exact similarity for 2 * 3 * 200 = 1200 vectors. candidates
     * must be set to a number greater or equal to the number of Elasticsearch results you want to get. Higher values
     * generally mean higher recall and higher latency.
     */
    private final int candidates;

    public String getSimilarityMetric() {
      return similarityMetric;
    }
    public float getSimilarity() {
      return similarity;
    }

    public int getK() {
      return k;
    }

    public int getCandidates() {
      return candidates;
    }
  }
}
