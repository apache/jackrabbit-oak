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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public class ReadOnlyRevisions implements Revisions, Closeable {

    private static final Logger LOG = LoggerFactory
            .getLogger(ReadOnlyRevisions.class);

    public static final String JOURNAL_FILE_NAME = "journal.log";

    @Nonnull
    private final AtomicReference<RecordId> head;

    @Nonnull
    private final File directory;

    @Nonnull
    private final RandomAccessFile journalFile;

    public ReadOnlyRevisions(@Nonnull File directory) throws IOException {
        this.directory = checkNotNull(directory);
        this.journalFile = new RandomAccessFile(new File(directory,
                JOURNAL_FILE_NAME), "r");
        this.journalFile.seek(journalFile.length());
        this.head = new AtomicReference<>(null);
    }

    /**
     * Bind this instance to a store.
     * 
     * @param store
     *            store to bind to
     * @param writeInitialNode
     *            provider for the initial node in case the journal is empty.
     * @throws IOException
     */
    synchronized void bind(@Nonnull SegmentStore store) throws IOException {
        if (head.get() == null) {
            RecordId persistedId = null;
            try (JournalReader journalReader = new JournalReader(new File(
                    directory, JOURNAL_FILE_NAME))) {
                while (persistedId == null && journalReader.hasNext()) {
                    String entry = journalReader.next();
                    try {
                        RecordId id = RecordId.fromString(store, entry);
                        if (store.containsSegment(id.getSegmentId())) {
                            persistedId = id;
                        } else {
                            LOG.warn(
                                    "Unable to access revision {}, rewinding...",
                                    id);
                        }
                    } catch (IllegalArgumentException ignore) {
                        LOG.warn("Skipping invalid record id {}", entry);
                    }
                }
            }

            if (persistedId == null) {
                throw new IllegalStateException(
                        "Cannot start readonly store from empty journal");
            } else {
                head.set(persistedId);
            }
        }
    }

    private void checkBound() {
        checkState(head.get() != null, "Revisions not bound to a store");
    }

    @Nonnull
    @Override
    public RecordId getHead() {
        checkBound();
        return head.get();
    }

    @Override
    public boolean setHead(RecordId expected, RecordId head, Option... options) {
        checkBound();
        RecordId id = this.head.get();
        return id.equals(expected) && this.head.compareAndSet(id, head);
    }

    @Override
    public boolean setHead(Function<RecordId, RecordId> newHead,
            Option... options) throws InterruptedException {
        throw new UnsupportedOperationException("ReadOnly Revisions");
    }

    /**
     * Close the underlying journal file.
     * 
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        journalFile.close();
    }

}
