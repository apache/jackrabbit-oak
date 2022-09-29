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
package org.apache.jackrabbit.oak.plugins.document.util;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.slf4j.Logger;

import java.util.function.BiPredicate;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isPreviousDocId;

/**
 * Statistics Util class.
 */
public class StatsCollectorUtil {

    private StatsCollectorUtil() {}

    public static void perfLog(final Logger perfLog, final int perfLogThreshold, long timeTakenNanos, String logMsgPrefix,
                               Object... arguments) {

        if (!perfLog.isDebugEnabled()){
            return;
        }

        final long diff = NANOSECONDS.toMillis(timeTakenNanos);
        if (perfLog.isTraceEnabled()) {
            // if log level is TRACE, then always log - and do that on TRACE
            // then:
            perfLog.trace(logMsgPrefix + " [took " + diff + "ms]", arguments);
        } else if (diff > perfLogThreshold) {
            perfLog.debug(logMsgPrefix + " [took " + diff + "ms]", arguments);
        }
    }

    public static BiStatsConsumer getStatsConsumer() {
        return (cJ, cJT, count, tTN) -> {
            cJ.mark(count);
            cJT.update(tTN / count, NANOSECONDS);
        };
    }

    public static BiStatsConsumer getJournalStatsConsumer() {
        return (cJ, cJT, count, tTN) -> {
            cJ.mark(count);
            cJT.update(tTN, NANOSECONDS);
        };
    }

    public static TriStatsConsumer getCreateStatsConsumer() {
        return (cNUM, cSNM, cNUT, ids1, tTN) -> {
            for (String id : ids1) {
                cNUM.mark();
                if (isPreviousDocId(id)) {
                    cSNM.mark();
                }
            }
            cNUT.update(tTN / ids1.size(), NANOSECONDS);
        };
    }

    public static BiPredicate<Collection<? extends Document>, Integer> isNodesCollectionUpdated() {
        return (c, i) -> c == NODES && i > 0;
    }
}
