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
 * A very simple estimator for no. of entries in the index using least mean square.
 */
class LMSEstimator {

    private double[] weights;

    public LMSEstimator(double[] weights) {
        this.weights = weights;
    }

    public LMSEstimator() {
        this.weights = new double[5];
    }

    synchronized void update(Filter filter, SolrDocumentList docs) {
        double[] updatedWeights = new double[weights.length];
        for (int i = 0; i < updatedWeights.length; i++) {
            double errors = (docs.getNumFound() - estimate(filter)) * getInput(filter, i);
            updatedWeights[i] = weights[i] + 0.03 * errors;
        }
        // weights updated
        weights = Arrays.copyOf(updatedWeights, 5);
    }

    long estimate(Filter filter) {
        long estimatedEntryCount = 0;
        for (int i = 0; i < 5; i++) {
            estimatedEntryCount += weights[i] * getInput(filter, i);
        }
        return estimatedEntryCount + 1; // smoothing
    }

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
