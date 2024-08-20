package org.apache.jackrabbit.oak.index.indexer.document;

import java.io.Closeable;

public interface AheadOfTimeBlobDownloader extends Closeable {

    void start();

    void updateIndexed(long lastEntryIndexed);

    AheadOfTimeBlobDownloader NOOP = new AheadOfTimeBlobDownloader() {
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
