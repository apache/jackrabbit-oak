/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import io.aeron.cluster.RecordingLog;
import org.agrona.concurrent.AgentTerminationException;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;

import java.io.File;
import java.util.function.LongSupplier;

/**
 * Detects that the Aeron log is not the one the Oak store was built from, using the store's applied watermark
 * (position P applied in term T). In the store's own log every term before or equal to T starts at or before P
 * and every later term starts at or after P, and the member's recording log holds term T.
 */
final class LogStoreGuard {

    private static final String REMEDY = " The Aeron cluster/ and archive/ directories were reset or replaced while "
        + "the Oak store was kept. Start this member with --fresh to rebuild the store from the cluster log, or "
        + "restore the cluster directory that belongs to this store.";

    private LogStoreGuard() {
    }

    /**
     * Called from onStart, while the consensus module waits for the service (it creates recording.log before).
     *
     * @param lastRecordedTerm last valid TERM entry in this member's recording log, or -1 if it has none
     */
    static void checkOnStart(AppliedLogPosition store, boolean hasSnapshot, LongSupplier lastRecordedTerm) {
        if (store.isNone()) {
            return;
        }
        long lastTerm = lastRecordedTerm.getAsLong();
        if (lastTerm < 0 && !hasSnapshot) {
            throw new IllegalStateException("Aeron log does not match this Oak store: the log is empty but the store "
                + "applied " + store + "." + REMEDY);
        }
        if (lastTerm < store.term()) {
            throw new IllegalStateException("Aeron log does not match this Oak store: the log ends in leadership term "
                + lastTerm + " but the store applied " + store + "." + REMEDY);
        }
    }

    /** Called for every NewLeadershipTermEvent, in log order, including replay. */
    static void checkTermEvent(AppliedLogPosition store, long leadershipTermId, long termBaseLogPosition) {
        if (store.isNone() || store.term() < 0) {
            return;
        }
        boolean laterTermStartsBeforeStore = leadershipTermId > store.term() && termBaseLogPosition < store.position();
        boolean earlierTermStartsAfterStore = leadershipTermId <= store.term() && termBaseLogPosition > store.position();
        if (laterTermStartsBeforeStore || earlierTermStartsAfterStore) {
            throw new AgentTerminationException("Aeron log does not match this Oak store: leadership term "
                + leadershipTermId + " starts at log position " + termBaseLogPosition + " but the store applied "
                + store + "." + REMEDY);
        }
    }

    static long lastRecordedTerm(File clusterDir) {
        if (clusterDir == null || !new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            return -1L;
        }
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            RecordingLog.Entry lastTerm = recordingLog.findLastTerm();
            return lastTerm != null ? lastTerm.leadershipTermId : -1L;
        }
    }
}
