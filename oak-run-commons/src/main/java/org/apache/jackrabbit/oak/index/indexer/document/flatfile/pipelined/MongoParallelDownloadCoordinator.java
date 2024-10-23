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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.bson.RawBsonDocument;
import org.jetbrains.annotations.NotNull;

/**
 * Coordinates the two parallel download streams used to download from Mongo when parallelDump is enabled. One stream
 * downloads in ascending order the other in descending order. This class keeps track of the top limit of the ascending
 * stream and of the bottom limit of the descending stream, and determines if the streams have crossed. This indicates
 * that the download completed and the two threads should stop.
 */
class MongoParallelDownloadCoordinator {

    public static class RawBsonDocumentWrapper {
        final RawBsonDocument rawBsonDocument;
        final long modified;
        final String id;

        public RawBsonDocumentWrapper(RawBsonDocument rawBsonDocument, long modified, String id) {
            this.rawBsonDocument = rawBsonDocument;
            this.modified = modified;
            this.id = id;
        }
    }

    static class DownloadPosition implements Comparable<DownloadPosition> {
        final long lastModified;
        final String lastId;

        public DownloadPosition(long lastModified, String lastId) {
            this.lastModified = lastModified;
            this.lastId = lastId;
        }

        @Override
        public int compareTo(@NotNull DownloadPosition o) {
            int lastModifiedComparison = Long.compare(lastModified, o.lastModified);
            if (lastModifiedComparison != 0) {
                return lastModifiedComparison;
            } else {
                return lastId.compareTo(o.lastId);
            }
        }

        @Override
        public String toString() {
            return "DownloadPosition{" +
                    "lastModified=" + lastModified +
                    ", lastId='" + lastId + '\'' +
                    '}';
        }
    }

    private DownloadPosition lowerRangeTop = new DownloadPosition(0, null);
    private DownloadPosition upperRangeBottom = new DownloadPosition(Long.MAX_VALUE, null);

    public DownloadPosition getUpperRangeBottom() {
        return upperRangeBottom;
    }

    public DownloadPosition getLowerRangeTop() {
        return lowerRangeTop;
    }

    /**
     * Extends the lower range of downloaded documents with the documents in the given batch and returns the index of
     * the first/lowest document in this batch that was already downloaded by the descending download thread.
     * <p>
     * That is, if this method returns i, then the documents in the range batch[0:i) were not yet downloaded by the
     * descending downloader, but batch[i] and above were already downloaded. The following are degenerate cases:
     * <p>
     * If i==0 then all documents of this batch were already downloaded. That is, b[0] >= upperRangeBottom.
     * If i==sizeOfBatch then none of the documents were downloaded. That is, b[sizeOfBatch-i] < upperRangeBottom.
     * <p>
     * <p>
     * The batch must be in ascending order of (_modified, _id).
     * <p>
     * Updates the lower range top to b[i].
     *
     * @param batch The batch of documents to be added to the lower range, must be in ascending order
     */
    public synchronized int extendLowerRange(RawBsonDocumentWrapper[] batch, int sizeOfBatch) {
        // batch must be in ascending order
        int i = sizeOfBatch - 1;
        // Start by the highest value in the range and compare it with the bottom of the upper range.
        while (i >= 0) {
            var bi = new DownloadPosition(batch[i].modified, batch[i].id);
            if (bi.compareTo(upperRangeBottom) < 0) {
                // batch[i] < upperRangeLowerLimit. Can add this element
                this.lowerRangeTop = bi;
                return i + 1;
            }
            // batch[i] >= lowerRangeTop, so it was already downloaded. Keep going down on this batch, trying to find
            // an element that was not yet downloaded
            i--;
        }

        // The whole batch block was already downloaded as part of the upper range
        return 0;
    }

    /**
     * Extends the upper range of downloaded documents with the documents in the given batch and returns the index of
     * the first/highest document in this batch that was already downloaded by the ascending download thread.
     *
     * @param batch The batch of documents to be added to the upper range, must be in descending order
     */
    public synchronized int extendUpperRange(RawBsonDocumentWrapper[] batch, int sizeOfBatch) {
        // batch must be in descending order
        int i = sizeOfBatch - 1;
        // Find the highest value in the batch that is not yet on the upper range of values downloaded
        while (i >= 0) {
            var bi = new DownloadPosition(batch[i].modified, batch[i].id);
            if (bi.compareTo(lowerRangeTop) > 0) {
                // batch[i] > upperRangeLowerLimit. Can add this element
                upperRangeBottom = bi;
                return i + 1;
            }
            i--;
        }
        // The whole batch block was already downloaded as part of the upper range
        return 0;
    }

    @Override
    public String toString() {
        return "MongoParallelDownloadCoordinator{" +
                "lowerRangeTop=" + lowerRangeTop +
                ", upperRangeBottom=" + upperRangeBottom +
                '}';
    }
}
