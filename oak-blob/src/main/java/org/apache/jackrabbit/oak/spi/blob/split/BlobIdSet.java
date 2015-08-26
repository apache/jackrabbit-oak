package org.apache.jackrabbit.oak.spi.blob.split;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

class BlobIdSet {

    private final File store;

    public BlobIdSet(String repositoryDir, String filename) {
        store = new File(new File(repositoryDir), filename);
    }

    public synchronized boolean contains(String blobId) throws IOException {
        if (!store.exists()) {
            return false;
        }
        final BufferedReader reader = new BufferedReader(new FileReader(store));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals(blobId)) {
                    return true;
                }
            }
        } finally {
            reader.close();
        }
        return false;
    }

    public synchronized void add(String blobId) throws IOException {
        final FileWriter writer = new FileWriter(store.getPath(), true);
        try {
            writer.append(blobId).append('\n');
        } finally {
            writer.close();
        }
    }
}
