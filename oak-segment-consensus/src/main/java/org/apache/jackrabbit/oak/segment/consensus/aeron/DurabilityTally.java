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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntSupplier;

/**
 * Replicated durability decision. Every member applies the same SEGMENT_PERSISTED log entries in the
 * same order, so every member decides a proposal (durable on a majority, or no longer able to be) at the
 * same log entry, including when the whole log is replayed into a fresh JVM.
 * Mutated only on the clustered service thread; {@link #hasReported} may be read from other threads.
 */
final class DurabilityTally {

    static final int MAX_TRACKED_PROPOSALS = 65_536;

    private final IntSupplier memberCount;
    private final Map<String, Votes> pending = boundedMap();
    private final Map<String, Outcome> decided = boundedMap();

    DurabilityTally(IntSupplier memberCount) {
        this.memberCount = memberCount;
    }

    /**
     * @return the decision if this entry decided the proposal, otherwise {@code null}
     */
    synchronized Outcome record(String proposalId, int memberId, boolean success, String durableHead, String error) {
        int members = memberCount.getAsInt();
        if (memberId < 0 || memberId >= members || decided.containsKey(proposalId)) {
            return null;
        }
        Votes votes = pending.computeIfAbsent(proposalId, id -> new Votes());
        if (success) {
            votes.failed.remove(memberId);
            votes.acked.add(memberId);
            if (votes.durableHead == null && durableHead != null && !durableHead.isEmpty()) {
                votes.durableHead = durableHead;
            }
        } else if (!votes.acked.contains(memberId)) {
            votes.failed.add(memberId);
            if (votes.error == null && error != null && !error.isEmpty()) {
                votes.error = error;
            }
        }

        int required = members / 2 + 1;
        Outcome outcome = null;
        if (votes.acked.size() >= required) {
            outcome = new Outcome(true, votes.durableHead, null);
        } else if (members - votes.failed.size() < required) {
            outcome = new Outcome(false, null, votes.error != null ? votes.error : "durability failed");
        }
        if (outcome != null) {
            pending.remove(proposalId);
            decided.put(proposalId, outcome);
        }
        return outcome;
    }

    /**
     * Whether the log already holds what {@code memberId} would report for the proposal.
     */
    synchronized boolean hasReported(String proposalId, int memberId) {
        if (decided.containsKey(proposalId)) {
            return true;
        }
        Votes votes = pending.get(proposalId);
        return votes != null && votes.acked.contains(memberId);
    }

    private static <V> Map<String, V> boundedMap() {
        return new LinkedHashMap<String, V>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, V> eldest) {
                return size() > MAX_TRACKED_PROPOSALS;
            }
        };
    }

    private static final class Votes {
        private final Set<Integer> acked = new HashSet<>();
        private final Set<Integer> failed = new HashSet<>();
        private String durableHead;
        private String error;
    }

    static final class Outcome {
        final boolean success;
        final String durableHead;
        final String error;

        private Outcome(boolean success, String durableHead, String error) {
            this.success = success;
            this.durableHead = durableHead;
            this.error = error;
        }
    }
}
