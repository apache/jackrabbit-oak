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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

interface GarbageCollectionStrategy {

    interface SuccessfulGarbageCollectionListener {

        void onSuccessfulGarbageCollection();

    }

    interface Context {

        SegmentGCOptions getGCOptions();

        GCListener getGCListener();

        Revisions getRevisions();

        GCJournal getGCJournal();

        SegmentTracker getSegmentTracker();

        SegmentWriterFactory getSegmentWriterFactory();

        GCNodeWriteMonitor getCompactionMonitor();

        BlobStore getBlobStore();

        Canceller getCanceller();

        long getLastSuccessfulGC();

        TarFiles getTarFiles();

        AtomicBoolean getSufficientMemory();

        FileReaper getFileReaper();

        SuccessfulGarbageCollectionListener getSuccessfulGarbageCollectionListener();

        SuccessfulCompactionListener getSuccessfulCompactionListener();

        Flusher getFlusher();

        long getGCBackOff();

        SegmentGCOptions.GCType getLastCompactionType();

        int getGCCount();

        SegmentCache getSegmentCache();

        FileStoreStats getFileStoreStats();

        SegmentReader getSegmentReader();

    }

    void collectGarbage(Context context) throws IOException;

    void collectFullGarbage(Context context) throws IOException;

    void collectTailGarbage(Context context) throws IOException;

    CompactionResult compactFull(Context context) throws IOException;

    CompactionResult compactTail(Context context) throws IOException;

    List<String> cleanup(Context context) throws IOException;

}
