/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

class BackgroundReadStats {
    CacheInvalidationStats cacheStats;
    long readHead;
    long cacheInvalidationTime;
    long populateDiffCache;
    long lock;
    long dispatchChanges;
    long totalReadTime;
    long numExternalChanges;
    long externalChangesLag;

    @Override
    public String toString() {
        String cacheStatsMsg = "NOP";
        if (cacheStats != null){
            cacheStatsMsg = cacheStats.summaryReport();
        }
        return  "ReadStats{" +
                "cacheStats:" + cacheStatsMsg +
                ", head:" + readHead +
                ", cache:" + cacheInvalidationTime +
                ", diff: " + populateDiffCache +
                ", lock:" + lock +
                ", dispatch:" + dispatchChanges +
                ", numExternalChanges:" + numExternalChanges +
                ", externalChangesLag:" + externalChangesLag+
                ", totalReadTime:" + totalReadTime +
                '}';
    }
}
