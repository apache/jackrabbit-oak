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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.jackrabbit.oak.segment.file.FileStoreUtil.findPersistedRecordId;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of {@code Revisions} is backed by a
 * {@link #JOURNAL_FILE_NAME journal} file where the current head is persisted
 * by calling {@link #tryFlush(Flusher)}.
 * <p>
 * The {@link #setHead(Function, Option...)} method supports a timeout
 * {@link Option}, which can be retrieved through factory methods of this class.
 * <p>
 * Instance of this class must be {@link #bind(SegmentStore, SegmentIdProvider, Supplier)} bound} to
 * a {@code SegmentStore} otherwise its method throw {@code IllegalStateException}s.
 */
public class TarRevisions implements Revisions, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TarRevisions.class);

    public static final String JOURNAL_FILE_NAME = "journal.log";

    /**
     * The lock protecting {@link #journalFile}.
     */
    private final Lock journalFileLock = new ReentrantLock();

    @Nonnull
    private final AtomicReference<RecordId> head;

    @Nonnull
    private final File directory;

    /**
     * The journal file. It is protected by {@link #journalFileLock}. It becomes
     * {@code null} after it's closed.
     */
    private volatile RandomAccessFile journalFile;

    /**
     * The persisted head of the root journal, used to determine whether the
     * latest {@link #head} value should be written to the disk.
     */
    @Nonnull
    private final AtomicReference<RecordId> persistedHead;

    @Nonnull
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    private static class TimeOutOption implements Option {
        private final long time;

        @Nonnull
        private final TimeUnit unit;

        TimeOutOption(long time, @Nonnull TimeUnit unit) {
            this.time = time;
            this.unit = unit;
        }

        @Nonnull
        public static TimeOutOption from(@CheckForNull Option option) {
            if (option instanceof TimeOutOption) {
                return (TimeOutOption) option;
            } else {
                throw new IllegalArgumentException("Invalid option " + option);
            }
        }
    }

    /**
     * Option to cause set head calls to be expedited. That is, cause them to skip the queue
     * of any other callers waiting to complete that don't have this option specified.
     */
    public static final Option EXPEDITE_OPTION = new Option() {
        @Override
        public String toString() {
            return "Expedite Option";
        }
    };

    /**
     * Timeout option approximating no time out ({@code Long.MAX_VALUE} days).
     */
    public static final Option INFINITY = new TimeOutOption(MAX_VALUE, DAYS);

    /**
     * Factory method for creating a timeout option.
     */
    public static Option timeout(long time, TimeUnit unit) {
        return new TimeOutOption(time, unit);
    }

    /**
     * Create a new instance placing the journal log file into the passed
     * {@code directory}.
     * @param directory     directory of the journal file
     * @throws IOException
     */
    public TarRevisions(@Nonnull File directory) throws IOException {
        this.directory = checkNotNull(directory);
        this.journalFile = new RandomAccessFile(new File(directory,
                JOURNAL_FILE_NAME), "rw");
        this.journalFile.seek(journalFile.length());
        this.head = new AtomicReference<>(null);
        this.persistedHead = new AtomicReference<>(null);
    }

    /**
     * Bind this instance to a store.
     * @param store              store to bind to
     * @param idProvider         {@code SegmentIdProvider} of the {@code store}
     * @param writeInitialNode   provider for the initial node in case the journal is empty.
     * @throws IOException
     */
    synchronized void bind(@Nonnull SegmentStore store,
                           @Nonnull SegmentIdProvider idProvider,
                           @Nonnull Supplier<RecordId> writeInitialNode)
    throws IOException {
        if (head.get() != null) {
            return;
        }
        RecordId persistedId = findPersistedRecordId(store, idProvider, new File(directory, JOURNAL_FILE_NAME));
        if (persistedId == null) {
            head.set(writeInitialNode.get());
        } else {
            persistedHead.set(persistedId);
            head.set(persistedId);
        }
    }

    private void checkBound() {
        checkState(head.get() != null, "Revisions not bound to a store");
    }

    /**
     * Flush the id of the current head to the journal after a call to {@code
     * persisted}. Differently from {@link #tryFlush(Flusher)}, this method
     * does not return early if a concurrent call is in progress. Instead, it
     * blocks the caller until the requested flush operation is performed.
     *
     * @param flusher call back for upstream dependencies to ensure the current
     *                head state is actually persisted before its id is written
     *                to the head state.
     */
    void flush(Flusher flusher) throws IOException {
        if (head.get() == null) {
            LOG.debug("No head available, skipping flush");
            return;
        }
        journalFileLock.lock();
        try {
            doFlush(flusher);
        } finally {
            journalFileLock.unlock();
        }
    }

    /**
     * Flush the id of the current head to the journal after a call to {@code
     * persisted}. This method does nothing and returns immediately if called
     * concurrently and a call is already in progress.
     *
     * @param flusher call back for upstream dependencies to ensure the current
     *                head state is actually persisted before its id is written
     *                to the head state.
     */
    void tryFlush(Flusher flusher) throws IOException {
        if (head.get() == null) {
            LOG.debug("No head available, skipping flush");
            return;
        }
        if (journalFileLock.tryLock()) {
            try {
                doFlush(flusher);
            } finally {
                journalFileLock.unlock();
            }
        } else {
            LOG.debug("Unable to lock the journal, skipping flush");
        }
    }

    private void doFlush(Flusher flusher) throws IOException {
        if (journalFile == null) {
            LOG.debug("No journal file available, skipping flush");
            return;
        }
        RecordId before = persistedHead.get();
        RecordId after = getHead();
        if (after.equals(before)) {
            LOG.debug("Head state did not change, skipping flush");
            return;
        }
        flusher.flush();
        LOG.debug("TarMK journal update {} -> {}", before, after);
        journalFile.writeBytes(after.toString10() + " root " + System.currentTimeMillis() + "\n");
        journalFile.getChannel().force(false);
        persistedHead.set(after);
    }

    @Nonnull
    @Override
    public RecordId getHead() {
        checkBound();
        return head.get();
    }
    
    @Nonnull
    @Override
    public RecordId getPersistedHead() {
        checkBound();
        return persistedHead.get();
    }

    /**
     * This implementation blocks if a concurrent call to
     * {@link #setHead(Function, Option...)} is already in
     * progress.
     *
     * @param options   zero or one expedite option for expediting this call
     * @throws IllegalArgumentException  on any non recognised {@code option}.
     * @see #EXPEDITE_OPTION
     */
    @Override
    public boolean setHead(
            @Nonnull RecordId expected,
            @Nonnull RecordId head,
            @Nonnull Option... options) {
        checkBound();

        // If the expedite option was specified we acquire the write lock instead of the read lock.
        // This will cause this thread to get the lock before all threads currently waiting to
        // enter the read lock. See also the class comment of ReadWriteLock.
        Lock lock = isExpedited(options)
            ? rwLock.writeLock()
            : rwLock.readLock();
        lock.lock();
        try {
            RecordId id = this.head.get();
            return id.equals(expected) && this.head.compareAndSet(id, head);
        } finally {
            lock.unlock();
        }
    }

    /**
     * This implementation blocks if a concurrent call is already in progress.
     * @param newHead  function mapping an record id to the record id to which
     *                 the current head id should be set. If it returns
     *                 {@code null} the head remains unchanged and {@code setHead}
     *                 returns {@code false}.

     * @param options  zero or one timeout options specifying how long to block
     * @throws InterruptedException
     * @throws IllegalArgumentException  on any non recognised {@code option}.
     * @see #timeout(long, TimeUnit)
     * @see #INFINITY
     */
    @Override
    public RecordId setHead(
            @Nonnull Function<RecordId, RecordId> newHead,
            @Nonnull Option... options)
    throws InterruptedException {
        checkBound();
        TimeOutOption timeout = getTimeout(options);
        if (rwLock.writeLock().tryLock(timeout.time, timeout.unit)) {
            try {
                RecordId after = newHead.apply(getHead());
                if (after != null) {
                    head.set(after);
                    return after;
                } else {
                    return null;
                }
            } finally {
                rwLock.writeLock().unlock();
            }
        } else {
            return null;
        }
    }

    private static boolean isExpedited(Option[] options) {
        if (options.length == 0) {
            return false;
        } else if (options.length == 1) {
            return options[0] == EXPEDITE_OPTION;
        } else {
            throw new IllegalArgumentException("Expected zero or one options, got " + options.length);
        }
    }

    @Nonnull
    private static TimeOutOption getTimeout(@Nonnull Option[] options) {
        if (options.length == 0) {
            return TimeOutOption.from(INFINITY);
        } else if (options.length == 1) {
            return TimeOutOption.from(options[0]);
        } else {
            throw new IllegalArgumentException("Expected zero or one options, got " + options.length);
        }
    }

    /**
     * Close the underlying journal file.
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        journalFileLock.lock();
        try {
            if (journalFile == null) {
                return;
            }
            journalFile.close();
            journalFile = null;
        } finally {
            journalFileLock.unlock();
        }
    }

}
