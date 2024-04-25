package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.jetbrains.annotations.NotNull;

/**
 * Coordinates the two parallel download streams used to download from Mongo when parallelDump is enabled. One stream
 * downloads in ascending order the other in descending order. This class keeps track of the top limit of the ascending
 * stream and of the bottom limit of the descending stream, and determines if the streams have crossed. This indidcates
 * that the download completed and the two threads should stop.
 */
class MongoParallelDownloadCoordinator {

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
     */
    public synchronized int extendLowerRange(NodeDocument[] batch, int sizeOfBatch) {
        // batch must be in ascending order
        int i = sizeOfBatch - 1;
        // Start by the highest value in the range and compare it with the bottom of the upper range.
        while (i >= 0) {
            var bi = new DownloadPosition(batch[i].getModified(), batch[i].getId());
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

    public synchronized int extendUpperRange(NodeDocument[] batch, int sizeOfBatch) {
        // batch must be in descending order
        int i = sizeOfBatch - 1;
        // Find the highest value in the batch that is not yet on the upper range of values downloaded
        while (i >= 0) {
            var bi = new DownloadPosition(batch[i].getModified(), batch[i].getId());
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
