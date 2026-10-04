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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks durability acknowledgments until an outcome is determined.
 * A completed success outcome is emitted again when a member that already acked acks again
 * (a replay), so a lost ACK_SEGMENT_PERSISTED can be recovered until {@link #complete} runs.
 */
final class DurabilityAckTracker {
    private static final Logger log = LoggerFactory.getLogger(DurabilityAckTracker.class);

    private final ConcurrentHashMap<String, PendingDurability> pending = new ConcurrentHashMap<>();

    void track(String proposalId, int totalMembers, int requiredAcks) {
        pending.compute(proposalId, (id, existing) -> {
            if (existing == null) {
                return new PendingDurability(totalMembers, requiredAcks);
            }
            existing.totalMembers = totalMembers;
            existing.requiredAcks = requiredAcks;
            return existing;
        });
    }

    Outcome record(String proposalId, int memberId, String durableHead, boolean success, String error,
                   int defaultTotalMembers, int defaultRequiredAcks) {
        if (memberId < 0) {
            return null;
        }
        boolean[] repeatedSuccessAck = {false};
        PendingDurability current = pending.compute(proposalId, (id, existing) -> {
            PendingDurability state = existing != null ? existing : new PendingDurability(defaultTotalMembers, defaultRequiredAcks);
            if (state.completed) {
                repeatedSuccessAck[0] = success && !state.ackedMembers.add(memberId);
                return state;
            }
            if (success) {
                state.ackedMembers.add(memberId);
                if (durableHead != null && !durableHead.isEmpty() && state.durableHead == null) {
                    state.durableHead = durableHead;
                }
            } else {
                state.failedMembers.add(memberId);
                if (error != null && !error.isEmpty() && state.lastError == null) {
                    state.lastError = error;
                }
            }
            return state;
        });

        if (current == null) {
            return null;
        }
        if (current.completed) {
            if (repeatedSuccessAck[0] && current.outcome.success) {
                log.info("Re-emitting durability ACK for proposal {} on repeated ack from member {}; "
                    + "the previous ACK was not applied", proposalId, memberId);
                return current.outcome;
            }
            log.debug("Dropping durability ack for completed proposal {} from member {} (success={})",
                proposalId, memberId, success);
            return null;
        }

        if (current.ackedMembers.size() >= current.requiredAcks) {
            current.outcome = new Outcome(true, true, current.durableHead, null, current.totalMembers, current.requiredAcks);
            current.completed = true;
            return current.outcome;
        }

        int maxPossibleSuccess = current.totalMembers - current.failedMembers.size();
        if (maxPossibleSuccess < current.requiredAcks) {
            current.outcome = new Outcome(true, false, current.durableHead, current.lastError, current.totalMembers, current.requiredAcks);
            current.completed = true;
            return current.outcome;
        }

        return new Outcome(false, false, current.durableHead, current.lastError, current.totalMembers, current.requiredAcks);
    }

    void complete(String proposalId) {
        pending.remove(proposalId);
    }

    private static final class PendingDurability {
        private volatile int totalMembers;
        private volatile int requiredAcks;
        private final Set<Integer> ackedMembers = ConcurrentHashMap.newKeySet();
        private final Set<Integer> failedMembers = ConcurrentHashMap.newKeySet();
        private volatile String durableHead;
        private volatile String lastError;
        private volatile boolean completed;
        private volatile Outcome outcome;

        private PendingDurability(int totalMembers, int requiredAcks) {
            this.totalMembers = totalMembers;
            this.requiredAcks = requiredAcks;
        }
    }

    static final class Outcome {
        final boolean shouldAck;
        final boolean success;
        final String durableHead;
        final String error;
        final int totalMembers;
        final int requiredAcks;

        private Outcome(boolean shouldAck, boolean success, String durableHead, String error,
                        int totalMembers, int requiredAcks) {
            this.shouldAck = shouldAck;
            this.success = success;
            this.durableHead = durableHead;
            this.error = error;
            this.totalMembers = totalMembers;
            this.requiredAcks = requiredAcks;
        }
    }
}
