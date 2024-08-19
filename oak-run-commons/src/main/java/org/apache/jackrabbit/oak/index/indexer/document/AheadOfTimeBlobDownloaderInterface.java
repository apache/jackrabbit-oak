package org.apache.jackrabbit.oak.index.indexer.document;

import java.io.Closeable;

public interface AheadOfTimeBlobDownloaderInterface extends Closeable {

    void start();

    void updateIndexed(long lastEntryIndexed);

    AheadOfTimeBlobDownloaderInterface NOOP = new AheadOfTimeBlobDownloaderInterface() {
        @Override
        public void start() {
        }

        @Override
        public void updateIndexed(long lastEntryIndexed) {
        }

        @Override
        public void close() {
        }
    };
}
