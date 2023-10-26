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

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;

public class VersionGCStatsAccessor {

    public static void addRun(VersionGCStats to, VersionGCStats add) {
        to.addRun(add);
    }

    public static int getUpdatedDetailedGCDocsCount(VersionGCStats stats) {
        return stats.updatedDetailedGCDocsCount;
    }

    public static long getDetailedGCDocsElapsed(VersionGCStats stats) {
        return stats.detailedGCDocsElapsed;
    }

    public static long getDeleteDetailedGCDocsElapsed(VersionGCStats stats) {
        return stats.deleteDetailedGCDocsElapsed;
    }

    public static boolean getIgnoredDetailedGCDueToCheckPoint(VersionGCStats stats) {
        return stats.ignoredDetailedGCDueToCheckPoint;
    }
}
