package org.apache.jackrabbit.oak.plugins.segment;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

public class CacheStore implements SegmentStore {

    private final SegmentStore store;

    private final LoadingCache<UUID, byte[]> cache;

    public CacheStore(final SegmentStore store, long cacheSize) {
        this.store = store;
        this.cache = CacheBuilder.newBuilder()
                .maximumWeight(cacheSize)
                .weigher(new Weigher<UUID, byte[]>() {
                    @Override
                    public int weigh(UUID key, byte[] value) {
                        return value.length;
                    }
                }).build(new CacheLoader<UUID, byte[]>() {
                    @Override
                    public byte[] load(UUID key) throws Exception {
                        return store.readSegment(key);
                    }
                });
    }

    @Override
    public RecordId getHead(String journal) {
        return store.getHead(journal);
    }

    @Override
    public boolean updateHead(String journal, RecordId base, RecordId head) {
        return store.updateHead(journal, base, head);
    }

    @Override
    public byte[] readSegment(UUID segmentId) {
        try {
            return cache.get(segmentId);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Failed to read segment " + segmentId, e);
        }
    }

    @Override
    public void createSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        store.createSegment(segmentId, data, offset, length);
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        store.deleteSegment(segmentId);
    }

}
