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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VersionGarbageCollector {
    private final DocumentNodeStore nodeStore;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private volatile long maxRevisionAge = TimeUnit.DAYS.toMillis(1);

    VersionGarbageCollector(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    public VersionGCStats gc() {
        VersionGCStats stats = new VersionGCStats();
        long oldestRevTimeStamp = nodeStore.getClock().getTime() - maxRevisionAge;

        //Check for any registered checkpoint which prevent the GC from running
        Revision checkpoint = nodeStore.getCheckpoints().getOldestRevisionToKeep();
        if (checkpoint != null && checkpoint.getTimestamp() < oldestRevTimeStamp) {
            log.info("Ignoring version gc as valid checkpoint [{}] found while " +
                            "need to collect versions older than [{}]", checkpoint.toReadableString(),
                    oldestRevTimeStamp
            );
            stats.ignoredGCDueToCheckPoint = true;
            return stats;
        }

        return stats;
    }

    public void setMaxRevisionAge(long maxRevisionAge) {
        this.maxRevisionAge = maxRevisionAge;
    }

    public static class VersionGCStats {
        boolean ignoredGCDueToCheckPoint;
    }
}
