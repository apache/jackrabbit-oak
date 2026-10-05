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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Replicated transaction boundaries. Every transition is applied from a log entry and uses only that
 * entry's cluster timestamp, so all members, and a full-log replay into a fresh JVM, decide alike:
 * the deadline is the START timestamp plus the timeout, and a transaction expires on its Aeron
 * TimerEvent or on the first COMMIT/ABORT entry stamped at or after the deadline, whichever the log
 * holds first. State lives only in memory and is rebuilt by replaying the log.
 */
final class TransactionLifecycleManager {

    private static final long DEFAULT_TIMEOUT_MS = 30_000L;
    private static final int DEFAULT_MAX_TERMINAL_ENTRIES = 10_000;

    private final int maxTerminalEntries;

    private final Map<String, TxRecord> active = new LinkedHashMap<>();
    private final Map<Long, String> activeByTimerId = new HashMap<>();
    private final LinkedHashMap<String, TxRecord> terminal = new LinkedHashMap<>();

    TransactionLifecycleManager() {
        this(DEFAULT_MAX_TERMINAL_ENTRIES);
    }

    TransactionLifecycleManager(int maxTerminalEntries) {
        this.maxTerminalEntries = Math.max(100, maxTerminalEntries);
    }

    /**
     * @param logTimestamp cluster timestamp of the START entry
     * @param timerId correlation id for the expiry timer, unique per log entry
     */
    synchronized TransitionResult onStart(String transactionId, String correlationId, long timeoutMs,
                                          String initiatorWallet, long logTimestamp, long timerId) {
        if (isBlank(transactionId)) {
            return TransitionResult.rejected("missing transactionId");
        }
        long effectiveTimeout = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;

        TxRecord activeRecord = active.get(transactionId);
        if (activeRecord != null) {
            return TransitionResult.idempotent(activeRecord.copy());
        }

        TxRecord terminalRecord = terminal.get(transactionId);
        if (terminalRecord != null) {
            return TransitionResult.rejected("transaction already terminal: " + terminalRecord.status);
        }

        TxRecord record = new TxRecord();
        record.transactionId = transactionId;
        record.correlationId = correlationId;
        record.initiatorWallet = initiatorWallet;
        record.status = TxStatus.STARTED;
        record.startedAtMs = logTimestamp;
        record.timeoutMs = effectiveTimeout;
        record.deadlineMs = logTimestamp + effectiveTimeout;
        record.timerId = timerId;
        active.put(transactionId, record);
        activeByTimerId.put(timerId, transactionId);
        return TransitionResult.applied(record.copy());
    }

    synchronized TransitionResult canStart(String transactionId) {
        if (isBlank(transactionId)) {
            return TransitionResult.rejected("missing transactionId");
        }
        TxRecord activeRecord = active.get(transactionId);
        if (activeRecord != null) {
            return TransitionResult.idempotent(activeRecord.copy());
        }
        TxRecord terminalRecord = terminal.get(transactionId);
        if (terminalRecord != null) {
            return TransitionResult.rejected("transaction already terminal: " + terminalRecord.status);
        }
        return TransitionResult.applied(null);
    }

    synchronized TransitionResult onCommit(String transactionId, String correlationId, long logTimestamp) {
        if (isBlank(transactionId)) {
            return TransitionResult.rejected("missing transactionId");
        }
        TxRecord activeRecord = active.get(transactionId);
        if (activeRecord != null) {
            if (logTimestamp >= activeRecord.deadlineMs) {
                return TransitionResult.timedOut(expire(activeRecord, logTimestamp));
            }
            finish(activeRecord, TxStatus.COMMITTED, logTimestamp);
            if (isBlank(activeRecord.correlationId)) {
                activeRecord.correlationId = correlationId;
            }
            return TransitionResult.applied(activeRecord.copy());
        }

        TxRecord terminalRecord = terminal.get(transactionId);
        if (terminalRecord == null) {
            return TransitionResult.rejected("unknown transaction");
        }
        if (terminalRecord.status == TxStatus.COMMITTED) {
            return TransitionResult.idempotent(terminalRecord.copy());
        }
        return TransitionResult.rejected("cannot commit terminal transaction: " + terminalRecord.status);
    }

    synchronized TransitionResult canCommit(String transactionId) {
        if (isBlank(transactionId)) {
            return TransitionResult.rejected("missing transactionId");
        }
        TxRecord activeRecord = active.get(transactionId);
        if (activeRecord != null) {
            return TransitionResult.applied(activeRecord.copy());
        }
        TxRecord terminalRecord = terminal.get(transactionId);
        if (terminalRecord == null) {
            return TransitionResult.rejected("unknown transaction");
        }
        if (terminalRecord.status == TxStatus.COMMITTED) {
            return TransitionResult.idempotent(terminalRecord.copy());
        }
        return TransitionResult.rejected("cannot commit terminal transaction: " + terminalRecord.status);
    }

    synchronized TransitionResult onAbort(String transactionId, String correlationId, String reason, long logTimestamp) {
        if (isBlank(transactionId)) {
            return TransitionResult.rejected("missing transactionId");
        }
        TxRecord activeRecord = active.get(transactionId);
        if (activeRecord != null) {
            if (logTimestamp >= activeRecord.deadlineMs) {
                return TransitionResult.timedOut(expire(activeRecord, logTimestamp));
            }
            activeRecord.abortReason = reason;
            if (isBlank(activeRecord.correlationId)) {
                activeRecord.correlationId = correlationId;
            }
            finish(activeRecord, TxStatus.ABORTED, logTimestamp);
            return TransitionResult.applied(activeRecord.copy());
        }

        TxRecord terminalRecord = terminal.get(transactionId);
        if (terminalRecord == null) {
            TxRecord syntheticAbort = new TxRecord();
            syntheticAbort.transactionId = transactionId;
            syntheticAbort.correlationId = correlationId;
            syntheticAbort.status = TxStatus.ABORTED;
            syntheticAbort.startedAtMs = logTimestamp;
            syntheticAbort.completedAtMs = logTimestamp;
            syntheticAbort.deadlineMs = logTimestamp;
            syntheticAbort.timeoutMs = 0L;
            syntheticAbort.abortReason = reason;
            addTerminal(syntheticAbort);
            return TransitionResult.applied(syntheticAbort.copy());
        }

        if (terminalRecord.status == TxStatus.ABORTED || terminalRecord.status == TxStatus.TIMED_OUT) {
            return TransitionResult.idempotent(terminalRecord.copy());
        }
        return TransitionResult.rejected("cannot abort terminal transaction: " + terminalRecord.status);
    }

    synchronized TransitionResult canAbort(String transactionId) {
        if (isBlank(transactionId)) {
            return TransitionResult.rejected("missing transactionId");
        }
        TxRecord activeRecord = active.get(transactionId);
        if (activeRecord != null) {
            return TransitionResult.applied(activeRecord.copy());
        }
        TxRecord terminalRecord = terminal.get(transactionId);
        if (terminalRecord == null) {
            return TransitionResult.applied(null);
        }
        if (terminalRecord.status == TxStatus.ABORTED || terminalRecord.status == TxStatus.TIMED_OUT) {
            return TransitionResult.idempotent(terminalRecord.copy());
        }
        return TransitionResult.rejected("cannot abort terminal transaction: " + terminalRecord.status);
    }

    /**
     * Applies an expiry TimerEvent; a timer whose transaction already ended is a no-op.
     */
    synchronized TransitionResult onTimer(long timerId, long logTimestamp) {
        String transactionId = activeByTimerId.get(timerId);
        TxRecord activeRecord = transactionId != null ? active.get(transactionId) : null;
        if (activeRecord == null) {
            return TransitionResult.rejected("no active transaction for timer " + timerId);
        }
        return TransitionResult.timedOut(expire(activeRecord, logTimestamp));
    }

    synchronized Optional<TxRecord> get(String transactionId) {
        TxRecord activeRecord = active.get(transactionId);
        if (activeRecord != null) {
            return Optional.of(activeRecord.copy());
        }
        TxRecord terminalRecord = terminal.get(transactionId);
        return terminalRecord != null ? Optional.of(terminalRecord.copy()) : Optional.empty();
    }

    synchronized Map<String, Object> stats() {
        long committed = 0;
        long aborted = 0;
        long timedOut = 0;
        for (TxRecord record : terminal.values()) {
            if (record.status == TxStatus.COMMITTED) {
                committed++;
            } else if (record.status == TxStatus.ABORTED) {
                aborted++;
            } else if (record.status == TxStatus.TIMED_OUT) {
                timedOut++;
            }
        }
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("active", active.size());
        stats.put("terminal", terminal.size());
        stats.put("committed", committed);
        stats.put("aborted", aborted);
        stats.put("timedOut", timedOut);
        return stats;
    }

    private TxRecord expire(TxRecord record, long logTimestamp) {
        record.abortReason = "timeout";
        finish(record, TxStatus.TIMED_OUT, logTimestamp);
        return record.copy();
    }

    private void finish(TxRecord record, TxStatus status, long logTimestamp) {
        active.remove(record.transactionId);
        activeByTimerId.remove(record.timerId);
        record.status = status;
        record.completedAtMs = logTimestamp;
        addTerminal(record);
    }

    private void addTerminal(TxRecord record) {
        terminal.put(record.transactionId, record);
        while (terminal.size() > maxTerminalEntries) {
            String eldest = terminal.keySet().iterator().next();
            terminal.remove(eldest);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    enum TxStatus {
        STARTED,
        COMMITTED,
        ABORTED,
        TIMED_OUT
    }

    static final class TransitionResult {
        private final boolean applied;
        private final boolean idempotent;
        private final boolean timedOut;
        private final String reason;
        private final TxRecord record;

        private TransitionResult(boolean applied, boolean idempotent, boolean timedOut, String reason, TxRecord record) {
            this.applied = applied;
            this.idempotent = idempotent;
            this.timedOut = timedOut;
            this.reason = reason;
            this.record = record;
        }

        static TransitionResult applied(TxRecord record) {
            return new TransitionResult(true, false, false, null, record);
        }

        static TransitionResult idempotent(TxRecord record) {
            return new TransitionResult(false, true, false, null, record);
        }

        static TransitionResult rejected(String reason) {
            return new TransitionResult(false, false, false, reason, null);
        }

        /** This entry expired the transaction instead of applying the requested transition. */
        static TransitionResult timedOut(TxRecord record) {
            return new TransitionResult(false, false, true, "transaction timed out", record);
        }

        boolean isApplied() {
            return applied;
        }

        boolean isIdempotent() {
            return idempotent;
        }

        boolean isTimedOut() {
            return timedOut;
        }

        String getReason() {
            return reason;
        }

        TxRecord getRecord() {
            return record;
        }
    }

    static final class TxRecord {
        String transactionId;
        String correlationId;
        String initiatorWallet;
        TxStatus status;
        long startedAtMs;
        long timeoutMs;
        long deadlineMs;
        long completedAtMs;
        long timerId;
        String abortReason;

        TxRecord copy() {
            TxRecord copy = new TxRecord();
            copy.transactionId = transactionId;
            copy.correlationId = correlationId;
            copy.initiatorWallet = initiatorWallet;
            copy.status = status;
            copy.startedAtMs = startedAtMs;
            copy.timeoutMs = timeoutMs;
            copy.deadlineMs = deadlineMs;
            copy.completedAtMs = completedAtMs;
            copy.timerId = timerId;
            copy.abortReason = abortReason;
            return copy;
        }
    }
}
