package org.apache.jackrabbit.oak.spi.blob.split;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

class BlobIdSet {

    private static final Logger log = LoggerFactory.getLogger(BlobIdSet.class);

    private final File store;

    private final BloomFilter<CharSequence> bloomFilter;

    private final Cache<String, Boolean> cache;

    BlobIdSet(String repositoryDir, String filename) {
        store = new File(new File(repositoryDir), filename);
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 50000);
        cache = CacheBuilder.newBuilder().maximumSize(1000).build();
        fillBloomFilter();
    }

    synchronized boolean contains(String blobId) throws IOException {
        if (!bloomFilter.apply(blobId)) {
            return false;
        }
        final Boolean cached = cache.getIfPresent(blobId);
        if (cached != null) {
            return cached;
        }

        if (isPresentInStore(blobId)) {
            cache.put(blobId, Boolean.TRUE);
            bloomFilter.put(blobId);
            return true;
        } else {
            cache.put(blobId, Boolean.FALSE);
            return false;
        }
    }

    synchronized void add(String blobId) throws IOException {
        addToStore(blobId);
        bloomFilter.put(blobId);
        cache.put(blobId, Boolean.TRUE);
    }

    private boolean isPresentInStore(String blobId) throws FileNotFoundException, IOException {
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

    private void addToStore(String blobId) throws IOException {
        final FileWriter writer = new FileWriter(store.getPath(), true);
        try {
            writer.append(blobId).append('\n');
        } finally {
            writer.close();
        }
    }

    private void fillBloomFilter() {
        if (!store.exists()) {
            return;
        }
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(store));
            String line;
            while ((line = reader.readLine()) != null) {
                bloomFilter.put(line);
            }
        } catch (IOException e) {
            log.error("Can't fill bloom filter", e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }
}
