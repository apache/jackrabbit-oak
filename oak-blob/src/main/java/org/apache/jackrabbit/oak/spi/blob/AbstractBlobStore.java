/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.blob;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.cache.Cache;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.StringUtils;

/**
 * An abstract data store that splits the binaries in relatively small blocks,
 * so that each block fits in memory.
 * <p>
 * Each data store id is a list of zero or more entries. Each entry is either
 * <ul>
 * <li>data (a number of bytes), or</li>
 * <li>the hash code of the content of a number of bytes, or</li>
 * <li>the hash code of the content of a data store id (indirect hash)</li>
 * </ul>
 * Thanks to the indirection, blocks can be kept relatively small, so that
 * caching is simpler, and so that the storage backend doesn't need to support
 * arbitrary size blobs (some storage backends buffer blobs in memory) and fast
 * seeks (some storage backends re-read the whole blob when seeking).
 * <p>
 * The format of a 'data' entry is: type (one byte; 0 for data), length
 * (variable size int), data (bytes).
 * <p>
 * The format of a 'hash of content' entry is: type (one byte; 1 for hash),
 * level (variable size int, 0 meaning not nested), size (variable size long),
 * hash code length (variable size int), hash code.
 * <p>
 * The format of a 'hash of data store id' entry is: type (one byte; 1 for
 * hash), level (variable size int, nesting level), total size (variable size
 * long), size of data store id (variable size long), hash code length (variable
 * size int), hash code.
 */
public abstract class AbstractBlobStore implements GarbageCollectableBlobStore, Cache.Backend<AbstractBlobStore.BlockId, AbstractBlobStore.Data> {

    protected static final String HASH_ALGORITHM = "SHA-256";

    protected static final int TYPE_DATA = 0;
    protected static final int TYPE_HASH = 1;
    protected static final int TYPE_HASH_COMPRESSED = 2;

    protected static final int BLOCK_SIZE_LIMIT = 48;

    protected Map<String, WeakReference<String>> inUse =
        Collections.synchronizedMap(new WeakHashMap<String, WeakReference<String>>());

    /**
     * The minimum size of a block. Smaller blocks are inlined (the data store id
     * is the data itself).
     */
    private int blockSizeMin = 4096;

    /**
     * The size of a block. 128 KB has been found to be as fast as larger
     * values, and faster than smaller values. 2 MB results in less files.
     */
    private int blockSize = 2 * 1024 * 1024;

    /**
     * The byte array is re-used if possible, to avoid having to create a new,
     * large byte array each time a (potentially very small) binary is stored.
     */
    private AtomicReference<byte[]> blockBuffer = new AtomicReference<byte[]>();

    public void setBlockSizeMin(int x) {
        validateBlockSize(x);
        this.blockSizeMin = x;
    }

    @Override
    public long getBlockSizeMin() {
        return blockSizeMin;
    }

    @Override
    public void setBlockSize(int x) {
        validateBlockSize(x);
        this.blockSize = x;
    }

    private static void validateBlockSize(int x) {
        if (x < BLOCK_SIZE_LIMIT) {
            throw new IllegalArgumentException(
                    "The minimum size must be bigger " + 
                    "than a content hash itself; limit = " + BLOCK_SIZE_LIMIT);
        }
    }

    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public String writeBlob(String tempFilePath) throws IOException {
        File file = new File(tempFilePath);
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            return writeBlob(in);
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(in);
            FileUtils.forceDelete(file);
        }
    }

    @Override
    public String writeBlob(InputStream in) throws IOException {
        try {
            ByteArrayOutputStream idStream = new ByteArrayOutputStream();
            convertBlobToId(in, idStream, 0, 0);
            byte[] id = idStream.toByteArray();
            // System.out.println("    write blob " +  StringUtils.convertBytesToHex(id));
            String blobId = StringUtils.convertBytesToHex(id);
            usesBlobId(blobId);
            return blobId;
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public InputStream getInputStream(String blobId) throws IOException {
        //Marking would handled by next call to store.readBlob
        return new BlobStoreInputStream(this, blobId, 0);
    }

    @Override
    public String getReference(String blobId) {
        return null;
    }

    @Override
    public String getBlobId(String reference) {
        return null;
    }

    protected void usesBlobId(String blobId) {
        inUse.put(blobId, new WeakReference<String>(blobId));
    }

    @Override
    public void clearInUse() {
        inUse.clear();
    }

    private void convertBlobToId(InputStream in, ByteArrayOutputStream idStream, int level, long totalLength) throws IOException {
        int count = 0;
        // try to re-use the block (but not concurrently)
        byte[] block = blockBuffer.getAndSet(null);
        if (block == null || block.length != blockSize) {
            // not yet initialized yet, already in use, or wrong size:
            // create a new one
            block = new byte[blockSize];
        }
        while (true) {
            int blockLen = IOUtils.readFully(in, block, 0, block.length);
            count++;
            if (blockLen == 0) {
                break;
            } else if (blockLen < blockSizeMin) {
                idStream.write(TYPE_DATA);
                IOUtils.writeVarInt(idStream, blockLen);
                idStream.write(block, 0, blockLen);
                totalLength += blockLen;
            } else {
                MessageDigest messageDigest;
                try {
                    messageDigest = MessageDigest.getInstance(HASH_ALGORITHM);
                } catch (NoSuchAlgorithmException e) {
                    throw new IOException(e);
                }
                messageDigest.update(block, 0, blockLen);
                byte[] digest = messageDigest.digest();
                idStream.write(TYPE_HASH);
                IOUtils.writeVarInt(idStream, level);
                if (level > 0) {
                    IOUtils.writeVarLong(idStream, totalLength);
                }
                IOUtils.writeVarLong(idStream, blockLen);
                totalLength += blockLen;
                IOUtils.writeVarInt(idStream, digest.length);
                idStream.write(digest);
                storeBlock(digest, level, Arrays.copyOf(block, blockLen));
            }
            if (idStream.size() > blockSize / 2) {
                // convert large ids to a block, but ensure it can be stored as
                // one block (otherwise the indirection no longer works)
                byte[] idBlock = idStream.toByteArray();
                idStream.reset();
                convertBlobToId(new ByteArrayInputStream(idBlock), idStream, level + 1, totalLength);
                count = 1;
            }
        }
        // re-use the block
        blockBuffer.set(block);
        if (count > 0 && idStream.size() > blockSizeMin) {
            // at the very end, convert large ids to a block,
            // because large block ids are not handy
            // (specially if they are used to read data in small chunks)
            byte[] idBlock = idStream.toByteArray();
            idStream.reset();
            convertBlobToId(new ByteArrayInputStream(idBlock), idStream, level + 1, totalLength);
        }
        in.close();
    }

    /**
     * Store a block of data.
     * 
     * @param digest the content hash
     * @param level the indirection level (0 is for user data, 1 is a list of
     *            digests that point to user data, 2 is a list of digests that
     *            point to digests, and so on). This parameter is for
     *            informational use only, and it is not required to store it
     *            unless that's easy to achieve
     * @param data the data to be stored
     */
    protected abstract void storeBlock(byte[] digest, int level, byte[] data) throws IOException;

    @Override
    public abstract void startMark() throws IOException;

    @Override
    public abstract int sweep() throws IOException;

    protected abstract boolean isMarkEnabled();

    protected abstract void mark(BlockId id) throws Exception;

    protected void markInUse() throws IOException {
        for (String id : new ArrayList<String>(inUse.keySet())) {
            mark(id);
        }
    }

    @Override
    public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
        if (isMarkEnabled()) {
            mark(blobId);
        }
        byte[] id = StringUtils.convertHexToBytes(blobId);
        ByteArrayInputStream idStream = new ByteArrayInputStream(id);
        while (true) {
            int type = idStream.read();
            if (type == -1) {
                return -1;
            } else if (type == TYPE_DATA) {
                int len = IOUtils.readVarInt(idStream);
                if (pos < len) {
                    IOUtils.skipFully(idStream, (int) pos);
                    len -= pos;
                    if (length < len) {
                        len = length;
                    }
                    IOUtils.readFully(idStream, buff, off, len);
                    return len;
                }
                IOUtils.skipFully(idStream, len);
                pos -= len;
            } else if (type == TYPE_HASH) {
                int level = IOUtils.readVarInt(idStream);
                long totalLength = IOUtils.readVarLong(idStream);
                if (level > 0) {
                    // block length (ignored)
                    IOUtils.readVarLong(idStream);
                }
                byte[] digest = new byte[IOUtils.readVarInt(idStream)];
                IOUtils.readFully(idStream, digest, 0, digest.length);
                if (pos >= totalLength) {
                    pos -= totalLength;
                } else {
                    if (level > 0) {
                        byte[] block = readBlock(digest, 0);
                        idStream = new ByteArrayInputStream(block);
                    } else {
                        long readPos = pos - pos % blockSize;
                        byte[] block = readBlock(digest, readPos);
                        ByteArrayInputStream in = new ByteArrayInputStream(block);
                        IOUtils.skipFully(in, pos - readPos);
                        return IOUtils.readFully(in, buff, off, length);
                    }
                }
            } else {
                throw new IOException("Unknown blobs id type " + type + " for blob " + blobId);
            }
        }
    }

    byte[] readBlock(byte[] digest, long pos) {
        BlockId id = new BlockId(digest, pos);
        return load(id).data;
    }

    @Override
    public Data load(BlockId id) {
        byte[] data;
        try {
            data = readBlockFromBackend(id);
        } catch (Exception e) {
            throw new RuntimeException("failed to read block from backend, id " + id, e);
        }
        if (data == null) {
            throw new IllegalArgumentException("The block with id " + id + " was not found");
        }
        return new Data(data);
    }

    /**
     * Load the block from the storage backend. Returns null if the block was
     * not found.
     * 
     * @param id the block id
     * @return the block data, or null
     */
    protected abstract byte[] readBlockFromBackend(BlockId id) throws Exception;

    @Override
    public long getBlobLength(String blobId) throws IOException {
        if (isMarkEnabled()) {
            mark(blobId);
        }
        byte[] id = StringUtils.convertHexToBytes(blobId);
        ByteArrayInputStream idStream = new ByteArrayInputStream(id);
        long totalLength = 0;
        while (true) {
            int type = idStream.read();
            if (type == -1) {
                break;
            }
            if (type == TYPE_DATA) {
                int len = IOUtils.readVarInt(idStream);
                IOUtils.skipFully(idStream, len);
                totalLength += len;
            } else if (type == TYPE_HASH) {
                int level = IOUtils.readVarInt(idStream);
                totalLength += IOUtils.readVarLong(idStream);
                if (level > 0) {
                    // block length (ignored)
                    IOUtils.readVarLong(idStream);
                }
                int digestLength = IOUtils.readVarInt(idStream);
                IOUtils.skipFully(idStream, digestLength);
            } else {
                throw new IOException("Datastore id type " + type + " for blob " + blobId);
            }
        }
        return totalLength;
    }

    protected void mark(String blobId) throws IOException {
        try {
            byte[] id = StringUtils.convertHexToBytes(blobId);
            ByteArrayInputStream idStream = new ByteArrayInputStream(id);
            mark(idStream);
        } catch (Exception e) {
            throw new IOException("Mark failed for blob " + blobId, e);
        }
    }

    private void mark(ByteArrayInputStream idStream) throws Exception {
        while (true) {
            int type = idStream.read();
            if (type == -1) {
                return;
            } else if (type == TYPE_DATA) {
                int len = IOUtils.readVarInt(idStream);
                IOUtils.skipFully(idStream, len);
            } else if (type == TYPE_HASH) {
                int level = IOUtils.readVarInt(idStream);
                // totalLength
                IOUtils.readVarLong(idStream);
                if (level > 0) {
                    // block length (ignored)
                    IOUtils.readVarLong(idStream);
                }
                byte[] digest = new byte[IOUtils.readVarInt(idStream)];
                IOUtils.readFully(idStream, digest, 0, digest.length);
                BlockId id = new BlockId(digest, 0);
                mark(id);
                if (level > 0) {
                    byte[] block = readBlock(digest, 0);
                    idStream = new ByteArrayInputStream(block);
                    mark(idStream);
                }
            } else {
                throw new IOException("Unknown blobs id type " + type);
            }
        }
    }

    @Override
    public Iterator<String> resolveChunks(String blobId) throws IOException {
        return new ChunkIterator(blobId);
    }
    
    /**
     * A block id. Blocks are small enough to fit in memory, so they can be
     * cached.
     */
    public static class BlockId {

        final byte[] digest;
        final long pos;

        BlockId(byte[] digest, long pos) {
            this.digest = digest;
            this.pos = pos;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || !(other instanceof BlockId)) {
                return false;
            }
            BlockId o = (BlockId) other;
            return Arrays.equals(digest, o.digest) &&
                    pos == o.pos;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(digest) ^
                    (int) (pos >> 32) ^ (int) pos;
        }

        @Override
        public String toString() {
            return StringUtils.convertBytesToHex(digest) + "@" + pos;
        }

        public byte[] getDigest() {
            return digest;
        }

        public long getPos() {
            return pos;
        }

    }

    /**
     * The data for a block.
     */
    public static class Data implements Cache.Value {

        final byte[] data;

        Data(byte[] data) {
            this.data = data;
        }

        @Override
        public String toString() {
            String s = StringUtils.convertBytesToHex(data);
            return s.length() > 100 ? s.substring(0, 100) + ".. (len=" + data.length + ")" : s;
        }

        @Override
        public int getMemory() {
            return data.length;
        }

    }

    class ChunkIterator implements Iterator<String> {
        
        private final static int BATCH = 2048;
        private final ArrayDeque<String> queue;
        private final ArrayDeque<ByteArrayInputStream> streamsStack;

        public ChunkIterator(String blobId) {
            byte[] id = StringUtils.convertHexToBytes(blobId);
            ByteArrayInputStream idStream = new ByteArrayInputStream(id);
            queue = new ArrayDeque<String>(BATCH);
            streamsStack = new ArrayDeque<ByteArrayInputStream>();
            streamsStack.push(idStream);
        }

        @Override
        public boolean hasNext() {
            if (!queue.isEmpty()) {
                return true;
            }
            try {
                while ((queue.size() < BATCH)
                        && (streamsStack.peekFirst() != null)) {
                    ByteArrayInputStream idStream = streamsStack.peekFirst();

                    int type = idStream.read();
                    if (type == -1) {
                        streamsStack.pop();
                    } else if (type == TYPE_DATA) {
                        int len = IOUtils.readVarInt(idStream);
                        IOUtils.skipFully(idStream, len);
                    } else if (type == TYPE_HASH) {
                        int level = IOUtils.readVarInt(idStream);
                        // totalLength
                        IOUtils.readVarLong(idStream);
                        if (level > 0) {
                            // block length (ignored)
                            IOUtils.readVarLong(idStream);
                        }
                        byte[] digest = new byte[IOUtils.readVarInt(idStream)];
                        IOUtils.readFully(idStream, digest, 0, digest.length);
                        if (level > 0) {
                            queue.add(StringUtils.convertBytesToHex(digest));
                            byte[] block = readBlock(digest, 0);
                            idStream = new ByteArrayInputStream(block);
                            streamsStack.push(idStream);
                        } else {
                            queue.add(StringUtils.convertBytesToHex(digest));
                        }
                    } else {
                        break;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // Check now if ids available
            if (!queue.isEmpty()) {
                return true;
            }

            return false;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No data");
            } 
            return queue.remove();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }
}
