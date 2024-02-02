package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public abstract class IdMappingBlobStore implements BlobStore {

    private final MemoryBlobStore bs = new MemoryBlobStore();

    private final Map<String, String> ids = new HashMap<>();

    @Override
    public String writeBlob(InputStream inputStream) throws IOException {
        String in = bs.writeBlob(inputStream);
        String out = generateId();
        ids.put(out, in);
        return out;
    }

    @Override
    public String writeBlob(InputStream inputStream, BlobOptions options) throws IOException {
        return writeBlob(inputStream);
    }

    @Override
    public int readBlob(String s, long l, byte[] bytes, int i, int i1) throws IOException {
        return bs.readBlob(mapId(s), l, bytes, i, i1);
    }

    @Override
    public long getBlobLength(String s) throws IOException {
        return bs.getBlobLength(mapId(s));
    }

    @Override
    public InputStream getInputStream(String s) throws IOException {
        return bs.getInputStream(mapId(s));
    }

    @Override
    public String getBlobId(@NotNull String s) {
        return bs.getBlobId(s);
    }

    @Override
    public String getReference(@NotNull String s) {
        return bs.getBlobId(mapId(s));
    }

    @Override
    public void close() throws Exception {
        bs.close();
    }

    private String mapId(String in) {
        String out = ids.get(in);
        if (out == null) {
            throw new IllegalArgumentException("in");
        }
        return out;
    }

    protected abstract String generateId();
}
