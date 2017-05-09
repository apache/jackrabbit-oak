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

package org.apache.jackrabbit.oak.segment.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileStoreUtil {

    private static final Logger log = LoggerFactory.getLogger(FileStoreUtil.class);

    private FileStoreUtil() {
        // Prevent instantiation
    }

    /**
     * Traverse the journal until a record ID is found that exists in the
     * provided segment store.
     *
     * @param store   An instance of {@link SegmentStore}.
     * @param idProvider  The {@code SegmentIdProvider} of the {@code store}
     * @param journal Path to the journal file.
     * @return An instance of {@link RecordId}, or {@code null} if none could be
     * found.
     * @throws IOException If an I/O error occurs.
     */
    static RecordId findPersistedRecordId(SegmentStore store, SegmentIdProvider idProvider, File journal)
    throws IOException {
        try (JournalReader journalReader = new JournalReader(journal)) {
            while (journalReader.hasNext()) {
                JournalEntry entry = journalReader.next();
                try {
                    RecordId id = RecordId.fromString(idProvider, entry.getRevision());
                    if (store.containsSegment(id.getSegmentId())) {
                        return id;
                    }
                    log.warn("Unable to access revision {}, rewinding...", id);
                } catch (IllegalArgumentException ignore) {
                    log.warn("Skipping invalid record id {}", entry);
                }
            }
        }
        return null;
    }

    static boolean containSegment(List<TarReader> readers, SegmentId id) {
        return containSegment(readers, id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    /**
     * Check if a segment is contained in one of the provided TAR files.
     *
     * @param readers A list of {@link TarReader} instances.
     * @param msb     Most significant bits of the segment ID.
     * @param lsb     Least significant bits of the segment ID.
     * @return {@code true} if the segment is contained in at least one of the
     * provided TAR files, {@code false} otherwise.
     */
    static boolean containSegment(List<TarReader> readers, long msb, long lsb) {
        for (TarReader reader : readers) {
            if (reader.containsEntry(msb, lsb)) {
                return true;
            }
        }
        return false;
    }

    static ByteBuffer readEntry(List<TarReader> readers, SegmentId id) {
        return readEntry(readers, id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    /**
     * Read the entry corresponding to a segment from one of the provided TAR
     * files.
     *
     * @param readers A list of {@link TarReader} instances.
     * @param msb     Most significant bits of the segment ID.
     * @param lsb     Least significant bits of the segment ID.
     * @return An instance of {@link ByteBuffer} if the entry for the segment
     * could be found, {@code null} otherwise.
     */
    static ByteBuffer readEntry(List<TarReader> readers, long msb, long lsb) {
        for (TarReader reader : readers) {
            if (reader.isClosed()) {
                log.debug("Skipping closed tar file {}", reader);
                continue;
            }
            try {
                ByteBuffer buffer = reader.readEntry(msb, lsb);
                if (buffer != null) {
                    return buffer;
                }
            } catch (IOException e) {
                log.warn("Failed to read from tar file {}", reader, e);
            }
        }
        return null;
    }

}
