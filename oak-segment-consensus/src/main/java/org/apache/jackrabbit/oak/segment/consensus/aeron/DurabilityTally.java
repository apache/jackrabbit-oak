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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.ToLongFunction;

/**
 * Replicated durability decision. Every member applies the same SEGMENT_PERSISTED log entries in the
 * same order, so every member decides a proposal (durable on a majority, or no longer able to be) at the
 * same log entry, including when the whole log is replayed into a fresh JVM.
 * A proposal is forgotten {@link #RETENTION_MS} of log time after its first report or its decision, so forgetting
 * is decided by the log as well. The tally is part of the Aeron snapshot, so a member restored from a snapshot
 * continues from the same state as the members that applied the log before it.
 * Mutated only on the clustered service thread; {@link #hasReported} may be read from other threads.
 */
final class DurabilityTally {

    static final int MAX_TRACKED_PROPOSALS = 65_536;
    /**
     * Matches the proposal queue's terminal retention: the proposer stops retrying a proposal awaiting
     * durability within minutes and forgets it after ten, so a later decision is never observed.
     */
    static final long RETENTION_MS = 10 * 60 * 1000L;

    private final IntSupplier memberCount;
    private final Map<String, Votes> pending = boundedMap();
    private final Map<String, Outcome> decided = boundedMap();

    DurabilityTally(IntSupplier memberCount) {
        this.memberCount = memberCount;
    }

    /**
     * @param clusterTime timestamp of the log entry, in milliseconds
     * @return the decision if this entry decided the proposal, otherwise {@code null}
     */
    synchronized Outcome record(String proposalId, int memberId, boolean success, String durableHead, String error,
                                long clusterTime) {
        expire(pending, votes -> votes.firstReportAt, clusterTime);
        expire(decided, outcome -> outcome.decidedAt, clusterTime);
        int members = memberCount.getAsInt();
        if (memberId < 0 || memberId >= members || decided.containsKey(proposalId)) {
            return null;
        }
        Votes votes = pending.computeIfAbsent(proposalId, id -> new Votes(clusterTime));
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
            outcome = new Outcome(true, votes.durableHead, null, clusterTime);
        } else if (members - votes.failed.size() < required) {
            outcome = new Outcome(false, null, votes.error != null ? votes.error : "durability failed", clusterTime);
        }
        if (outcome != null) {
            pending.remove(proposalId);
            decided.put(proposalId, outcome);
        }
        return outcome;
    }

    /**
     * @return the decision for the proposal, or {@code null} if it is undecided or no longer tracked
     */
    synchronized Outcome decision(String proposalId) {
        return decided.get(proposalId);
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

    /**
     * The tracked proposals as JSON values, undecided then decided, each in log order, for the Aeron snapshot.
     */
    synchronized List<Map<String, Object>> snapshotEntries() {
        List<Map<String, Object>> entries = new ArrayList<>(pending.size() + decided.size());
        pending.forEach((proposalId, votes) -> {
            Map<String, Object> entry = entry(proposalId, votes.firstReportAt, votes.durableHead, votes.error);
            entry.put("acked", new ArrayList<>(votes.acked));
            entry.put("failed", new ArrayList<>(votes.failed));
            entries.add(entry);
        });
        decided.forEach((proposalId, outcome) -> {
            Map<String, Object> entry = entry(proposalId, outcome.decidedAt, outcome.durableHead, outcome.error);
            entry.put("success", outcome.success);
            entries.add(entry);
        });
        return entries;
    }

    /**
     * Replaces the tracked proposals with those of {@link #snapshotEntries()}.
     */
    synchronized void restore(List<Map<String, Object>> entries) {
        pending.clear();
        decided.clear();
        for (Map<String, Object> entry : entries) {
            String proposalId = (String) entry.get("proposalId");
            long since = ((Number) entry.get("since")).longValue();
            String durableHead = (String) entry.get("durableHead");
            String error = (String) entry.get("error");
            Object success = entry.get("success");
            if (success instanceof Boolean) {
                decided.put(proposalId, new Outcome((Boolean) success, durableHead, error, since));
                continue;
            }
            Votes votes = new Votes(since);
            members(entry.get("acked"), votes.acked);
            members(entry.get("failed"), votes.failed);
            votes.durableHead = durableHead;
            votes.error = error;
            pending.put(proposalId, votes);
        }
    }

    private static Map<String, Object> entry(String proposalId, long since, String durableHead, String error) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("proposalId", proposalId);
        entry.put("since", since);
        entry.put("durableHead", durableHead);
        entry.put("error", error);
        return entry;
    }

    private static void members(Object json, Set<Integer> into) {
        for (Object member : (List<?>) json) {
            into.add(((Number) member).intValue());
        }
    }

    /** Entries are inserted in log order, so the eldest come first. */
    private static <V> void expire(Map<String, V> map, ToLongFunction<V> trackedSince, long clusterTime) {
        Iterator<V> eldestFirst = map.values().iterator();
        while (eldestFirst.hasNext() && clusterTime - trackedSince.applyAsLong(eldestFirst.next()) >= RETENTION_MS) {
            eldestFirst.remove();
        }
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
        private final long firstReportAt;
        private final Set<Integer> acked = new HashSet<>();
        private final Set<Integer> failed = new HashSet<>();
        private String durableHead;
        private String error;

        private Votes(long firstReportAt) {
            this.firstReportAt = firstReportAt;
        }
    }

    static final class Outcome {
        final boolean success;
        final String durableHead;
        final String error;
        private final long decidedAt;

        private Outcome(boolean success, String durableHead, String error, long decidedAt) {
            this.success = success;
            this.durableHead = durableHead;
            this.error = error;
            this.decidedAt = decidedAt;
        }
    }
}
