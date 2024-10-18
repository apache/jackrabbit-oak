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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.segment.file.FileStoreUtil.findPersistedRecordId;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.jetbrains.annotations.NotNull;

public class ReadOnlyRevisions implements Revisions, Closeable {

    @NotNull
    private final AtomicReference<RecordId> head;

    @NotNull
    private final JournalFile journalFile;

    public ReadOnlyRevisions(@NotNull SegmentNodeStorePersistence persistence) {
        this.journalFile = requireNonNull(persistence).getJournalFile();
        this.head = new AtomicReference<>(null);
    }

    /**
     * Bind this instance to a store.
     *
     * @param store store to bind to
     * @param idProvider  {@code SegmentIdProvider} of the {@code store}
     * @throws IOException
     */
    synchronized void bind(@NotNull SegmentStore store, @NotNull SegmentIdProvider idProvider)
    throws IOException {
        if (head.get() != null) {
            return;
        }
        RecordId persistedId = findPersistedRecordId(store, idProvider, journalFile);
        if (persistedId == null) {
            throw new IllegalStateException("Cannot start readonly store from empty journal");
        }
        head.set(persistedId);
    }

    private void checkBound() {
        Validate.checkState(head.get() != null, "Revisions not bound to a store");
    }

    @NotNull
    @Override
    public RecordId getHead() {
        checkBound();
        return head.get();
    }

    @NotNull
    @Override
    public RecordId getPersistedHead() {
        return getHead();
    }

    @Override
    public boolean setHead(@NotNull RecordId expected, @NotNull RecordId head, @NotNull Option... options) {
        checkBound();
        RecordId id = this.head.get();
        return id.equals(expected) && this.head.compareAndSet(id, head);
    }

    @Override
    public RecordId setHead(
            @NotNull Function<RecordId, RecordId> newHead,
            @NotNull Option... options) throws InterruptedException {
        throw new UnsupportedOperationException("ReadOnly Revisions");
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
