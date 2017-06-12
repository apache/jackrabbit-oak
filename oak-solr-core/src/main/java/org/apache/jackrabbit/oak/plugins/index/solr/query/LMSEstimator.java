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
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import java.util.Arrays;

import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.solr.common.SolrDocumentList;

/**
 * A very simple estimator for no. of entries in the index using least mean square update method for linear regression.
 */
class LMSEstimator {

    private static final double DEFAULT_ALPHA = 0.03;
    private static final int DEFAULT_THRESHOLD = 5;

    private double[] weights;
    private final double alpha;
    private final long threshold;

    LMSEstimator(double alpha, double[] weights, long threshold) {
        this.alpha = alpha;
        this.weights = weights;
        this.threshold = threshold;
    }

    LMSEstimator(double[] weights) {
        this(DEFAULT_ALPHA, weights, DEFAULT_THRESHOLD);
    }

    LMSEstimator() {
        this(DEFAULT_ALPHA, new double[5], 5);
    }

    synchronized void update(Filter filter, SolrDocumentList docs) {
        double[] updatedWeights = new double[weights.length];

        // least mean square cost
        long estimate = estimate(filter);
        long numFound = docs.getNumFound();
        long residual = numFound - estimate;
        double delta = Math.pow(residual, 2);

        if (Math.abs(delta) > threshold) {
            for (int i = 0; i < updatedWeights.length; i++) {
                updatedWeights[i] = weights[i] + alpha * residual * getInput(filter, i);
            }
            // weights updated
            weights = Arrays.copyOf(updatedWeights, 5);
        }
    }

    long estimate(Filter filter) {
        long estimatedEntryCount = 0;
        for (int i = 0; i < 5; i++) {
            estimatedEntryCount += weights[i] * getInput(filter, i);
        }
        return Math.max(0, estimatedEntryCount);
    }

    /**
     * Get the input value for a certain feature (by index) in the given filter.
     * <p/>
     * A filter is represented as a vector in R^5 where
     * i_0 : no. of property restrictions
     * i_1 : 1 if any native constraint exists in the filter, 0 otherwise
     * i_2 : the path restriction ordinal
     * i_3 : the depth of the path restriction if set, 0 otherwise
     * i_4 : the precedence of the dominant full text constraint if present, 0 otherwise
     *
     * @param filter the filter
     * @param i      the index of the filter vector feature to retrieve
     * @return the feature value
     */
    private long getInput(Filter filter, int i) {
        assert i < 5;
        if (i == 0) {
            return filter.getPropertyRestrictions() != null ? filter.getPropertyRestrictions().size() : 0;
        } else if (i == 1) {
            return filter.containsNativeConstraint() ? 1 : 0;
        } else if (i == 2) {
            return filter.getPathRestriction() != null ? filter.getPathRestriction().ordinal() : 0;
        } else if (i == 3) {
            return filter.getPathRestriction() != null ? filter.getPathRestriction().toString().split("/").length : 0;
        } else if (i == 4) {
            return filter.getFullTextConstraint() != null ? filter.getFullTextConstraint().getPrecedence() : 0;
        }
        return 0;
    }
}
