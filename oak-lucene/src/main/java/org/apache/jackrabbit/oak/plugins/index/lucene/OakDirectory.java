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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.WeakIdentityMap;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * Implementation of the Lucene {@link Directory} (a flat list of files)
 * based on an Oak {@link NodeBuilder}.
 */
public class OakDirectory extends Directory {
    static final PerfLogger PERF_LOGGER = new PerfLogger(LoggerFactory.getLogger(OakDirectory.class.getName() + ".perf"));
    static final String PROP_DIR_LISTING = "dirListing";
    static final String PROP_BLOB_SIZE = "blobSize";
    static final String PROP_UNIQUE_KEY = "uniqueKey";
    static final int UNIQUE_KEY_SIZE = 16;
    
    private final static SecureRandom secureRandom = new SecureRandom();
    
    protected final NodeBuilder builder;
    protected final NodeBuilder directoryBuilder;
    private final IndexDefinition definition;
    private LockFactory lockFactory;
    private final boolean readOnly;
    private final Set<String> fileNames = Sets.newConcurrentHashSet();
    private final boolean activeDeleteEnabled;
    private final String indexName;

    public OakDirectory(NodeBuilder builder, IndexDefinition definition, boolean readOnly) {
        this(builder, INDEX_DATA_CHILD_NAME, definition, readOnly);
    }

    public OakDirectory(NodeBuilder builder, String dataNodeName, IndexDefinition definition, boolean readOnly) {
        this.lockFactory = NoLockFactory.getNoLockFactory();
        this.builder = builder;
        this.directoryBuilder = readOnly ? builder.getChildNode(dataNodeName) : builder.child(dataNodeName);
        this.definition = definition;
        this.readOnly = readOnly;
        this.fileNames.addAll(getListing());
        this.activeDeleteEnabled = definition.getActiveDeleteEnabled();
        this.indexName = definition.getIndexName();
    }

    @Override
    public String[] listAll() throws IOException {
        return fileNames.toArray(new String[fileNames.size()]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return fileNames.contains(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        checkArgument(!readOnly, "Read only directory");
        fileNames.remove(name);
        NodeBuilder f = directoryBuilder.getChildNode(name);
        if (activeDeleteEnabled) {
            PropertyState property = f.getProperty(JCR_DATA);
            ArrayList<Blob> data;
            if (property != null && property.getType() == BINARIES) {
                data = newArrayList(property.getValue(BINARIES));
            } else {
                data = newArrayList();
            }
            NodeBuilder trash = builder.child(LuceneIndexConstants.TRASH_CHILD_NAME);
            long index;
            if (!trash.hasProperty("index")) {
                index = 1;
            } else {    
                index = trash.getProperty("index").getValue(Type.LONG) + 1;                
            }
            trash.setProperty("index", index);
            NodeBuilder trashEntry = trash.child("run_" + index);
            trashEntry.setProperty("time", System.currentTimeMillis());
            trashEntry.setProperty("name", name);
            trashEntry.setProperty(JCR_DATA, data, BINARIES);
        }
        f.remove();
    }

    @Override
    public long fileLength(String name) throws IOException {
        NodeBuilder file = directoryBuilder.getChildNode(name);
        OakIndexInput input = new OakIndexInput(name, file, indexName);
        try {
            return input.length();
        } finally {
            input.close();
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
            throws IOException {
        checkArgument(!readOnly, "Read only directory");
        NodeBuilder file;
        if (!directoryBuilder.hasChildNode(name)) {
            file = directoryBuilder.child(name);
            byte[] uniqueKey = new byte[UNIQUE_KEY_SIZE];
            secureRandom.nextBytes(uniqueKey);
            String key = StringUtils.convertBytesToHex(uniqueKey);
            file.setProperty(PROP_UNIQUE_KEY, key);
            file.setProperty(PROP_BLOB_SIZE, definition.getBlobSize());
        } else {
            file = directoryBuilder.child(name);
        }
        fileNames.add(name);
        return new OakIndexOutput(name, file, indexName);
    }


    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException {
        NodeBuilder file = directoryBuilder.getChildNode(name);
        if (file.exists()) {
            return new OakIndexInput(name, file, indexName);
        } else {
            String msg = String.format("[%s] %s", indexName, name);
            throw new FileNotFoundException(msg);
        }
    }

    @Override
    public Lock makeLock(String name) {
        return lockFactory.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        lockFactory.clearLock(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // ?
    }

    @Override
    public void close() throws IOException {
        if (!readOnly && definition.saveDirListing()) {
            directoryBuilder.setProperty(createProperty(PROP_DIR_LISTING, fileNames, STRINGS));
        }
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        this.lockFactory = lockFactory;
    }

    @Override
    public LockFactory getLockFactory() {
        return lockFactory;
    }

    @Override
    public String toString() {
        return "Directory for " + definition.getIndexName();
    }

    private Set<String> getListing(){
        long start = PERF_LOGGER.start();
        Iterable<String> fileNames = null;
        if (definition.saveDirListing()) {
            PropertyState listing = directoryBuilder.getProperty(PROP_DIR_LISTING);
            if (listing != null) {
                fileNames = listing.getValue(Type.STRINGS);
            }
        }

        if (fileNames == null){
            fileNames = directoryBuilder.getChildNodeNames();
        }
        Set<String> result = ImmutableSet.copyOf(fileNames);
        PERF_LOGGER.end(start, 100, "Directory listing performed. Total {} files", result.size());
        return result;
    }

    /**
     * Size of the blob entries to which the Lucene files are split.
     * Set to higher than the 4kB inline limit for the BlobStore,
     */
    static final int DEFAULT_BLOB_SIZE = 32 * 1024;

    /**
     * A file, which might be split into multiple blobs.
     */
    private static class OakIndexFile {

        /**
         * The file name.
         */
        private final String name;

        /**
         * The node that contains the data for this file.
         */
        private final NodeBuilder file;

        /**
         * The maximum size of each blob.
         */
        private final int blobSize;
        
        /**
         * The current position within the file (for positioned read and write
         * operations).
         */
        private long position = 0;

        /**
         * The length of the file.
         */
        private long length;

        /**
         * The list of blobs (might be empty).
         * The last blob has a size of 1 up to blobSize.
         * All other blobs have a size of blobSize.
         */
        private List<Blob> data;

        /**
         * Whether the data was modified since it was last flushed. If yes, on a
         * flush, the metadata, and the list of blobs need to be stored.
         */
        private boolean dataModified = false;

        /**
         * The index of the currently loaded blob.
         */
        private int index = -1;

        /**
         * The data of the currently loaded blob.
         */
        private byte[] blob;
        
        /**
         * The unique key that is used to make the content unique (to allow removing binaries from the blob store without risking to remove binaries that are still needed).
         */
        private final byte[] uniqueKey;

        /**
         * Whether the currently loaded blob was modified since the blob was
         * flushed.
         */
        private boolean blobModified = false;

        private final String dirDetails;

        public OakIndexFile(String name, NodeBuilder file, String dirDetails) {
            this.name = name;
            this.file = file;
            this.dirDetails = dirDetails;
            this.blobSize = determineBlobSize(file);
            this.uniqueKey = readUniqueKey(file);
            this.blob = new byte[blobSize];

            PropertyState property = file.getProperty(JCR_DATA);
            if (property != null && property.getType() == BINARIES) {
                this.data = newArrayList(property.getValue(BINARIES));
            } else {
                this.data = newArrayList();
            }

            this.length = (long)data.size() * blobSize;
            if (!data.isEmpty()) {
                Blob last = data.get(data.size() - 1);
                this.length -= blobSize - last.length();
                if (uniqueKey != null) {
                    this.length -= uniqueKey.length;
                }
            }
        }

        private OakIndexFile(OakIndexFile that) {
            this.name = that.name;
            this.file = that.file;
            this.dirDetails = that.dirDetails;
            this.blobSize = that.blobSize;
            this.uniqueKey = that.uniqueKey;
            this.blob = new byte[blobSize];

            this.position = that.position;
            this.length = that.length;
            this.data = newArrayList(that.data);
            this.dataModified = that.dataModified;
        }

        private void loadBlob(int i) throws IOException {
            checkElementIndex(i, data.size());
            if (index != i) {
                flushBlob();
                checkState(!blobModified);

                int n = (int) Math.min(blobSize, length - (long)i * blobSize);
                InputStream stream = data.get(i).getNewStream();
                try {
                    ByteStreams.readFully(stream, blob, 0, n);
                } finally {
                    stream.close();
                }
                index = i;
            }
        }

        private void flushBlob() throws IOException {
            if (blobModified) {
                int n = (int) Math.min(blobSize, length - (long)index * blobSize);
                InputStream in = new ByteArrayInputStream(blob, 0, n);
                if (uniqueKey != null) {
                    in = new SequenceInputStream(in, 
                            new ByteArrayInputStream(uniqueKey));
                }
                Blob b = file.createBlob(in);
                if (index < data.size()) {
                    data.set(index, b);
                } else {
                    checkState(index == data.size());
                    data.add(b);
                }
                dataModified = true;
                blobModified = false;
            }
        }

        public void seek(long pos) throws IOException {
            // seek() may be called with pos == length
            // see https://issues.apache.org/jira/browse/LUCENE-1196
            if (pos < 0 || pos > length) {
                String msg = String.format("Invalid seek request for [%s][%s], " +
                        "position: %d, file length: %d", dirDetails, name, pos, length);
                throw new IOException(msg);                
            } else {
                position = pos;
            }
        }

        public void readBytes(byte[] b, int offset, int len)
                throws IOException {
            checkPositionIndexes(offset, offset + len, checkNotNull(b).length);

            if (len < 0 || position + len > length) {
                String msg = String.format("Invalid byte range request for [%s][%s], " +
                        "position: %d, file length: %d, len: %d", dirDetails, name, position, length, len);
                throw new IOException(msg);
            }

            int i = (int) (position / blobSize);
            int o = (int) (position % blobSize);
            while (len > 0) {
                loadBlob(i);

                int l = Math.min(len, blobSize - o);
                System.arraycopy(blob, o, b, offset, l);

                offset += l;
                len -= l;
                position += l;
                // next block
                i++;
                // for the next block, we read from the beginning
                o = 0;
            }
        }

        public void writeBytes(byte[] b, int offset, int len)
                throws IOException {
            int i = (int) (position / blobSize);
            int o = (int) (position % blobSize);
            while (len > 0) {
                int l = Math.min(len, blobSize - o);

                if (index != i) {
                    if (o > 0 || (l < blobSize && position + l < length)) {
                        // loadBlob first flushes the previous block,
                        // and it sets the index
                        loadBlob(i);
                    } else {
                        // we don't need to load the block,
                        // as we anyway overwrite it fully, if:
                        // o == 0 (start writing at a block boundary)
                        // and either: l is the blockSize, or
                        // we write at least to the end of the file
                        flushBlob();
                        index = i;
                    }
                }
                System.arraycopy(b, offset, blob, o, l);
                blobModified = true;

                offset += l;
                len -= l;
                position += l;
                length = Math.max(length, position);

                i++;
                o = 0;
            }
        }

        private static int determineBlobSize(NodeBuilder file){
            if (file.hasProperty(PROP_BLOB_SIZE)){
                return Ints.checkedCast(file.getProperty(PROP_BLOB_SIZE).getValue(Type.LONG));
            }
            return DEFAULT_BLOB_SIZE;
        }

        private static byte[] readUniqueKey(NodeBuilder file) {
            if (file.hasProperty(PROP_UNIQUE_KEY)) {
                String key = file.getString(PROP_UNIQUE_KEY);
                return StringUtils.convertHexToBytes(key);
            }
            return null;
        }

        public void flush() throws IOException {
            flushBlob();
            if (dataModified) {
                file.setProperty(JCR_LASTMODIFIED, System.currentTimeMillis());
                file.setProperty(JCR_DATA, data, BINARIES);
                dataModified = false;
            }
        }

        @Override
        public String toString() {
            return name;
        }

        public String getName() {
            return name;
        }
    }

    private static class OakIndexInput extends IndexInput {

        private final OakIndexFile file;
        private boolean isClone = false;
        private final WeakIdentityMap<OakIndexInput, Boolean> clones;
        private final String dirDetails;

        public OakIndexInput(String name, NodeBuilder file, String dirDetails) {
            super(name);
            this.dirDetails = dirDetails;
            this.file = new OakIndexFile(name, file, dirDetails);
            clones = WeakIdentityMap.newConcurrentHashMap();
        }

        private OakIndexInput(OakIndexInput that) {
            super(that.toString());
            this.file = new OakIndexFile(that.file);
            clones = null;
            this.dirDetails = that.dirDetails;
        }

        @Override
        public OakIndexInput clone() {
            // TODO : shouldn't we call super#clone ?
            OakIndexInput clonedIndexInput = new OakIndexInput(this);
            clonedIndexInput.isClone = true;
            if (clones != null) {
                clones.put(clonedIndexInput, Boolean.TRUE);
            }
            return clonedIndexInput;
        }

        @Override
        public void readBytes(byte[] b, int o, int n) throws IOException {
            checkNotClosed();
            file.readBytes(b, o, n);
        }

        @Override
        public byte readByte() throws IOException {
            checkNotClosed();
            byte[] b = new byte[1];
            readBytes(b, 0, 1);
            return b[0];
        }

        @Override
        public void seek(long pos) throws IOException {
            checkNotClosed();
            file.seek(pos);
        }

        @Override
        public long length() {
            checkNotClosed();
            return file.length;
        }

        @Override
        public long getFilePointer() {
            checkNotClosed();
            return file.position;
        }

        @Override
        public void close() {
            file.blob = null;
            file.data = null;

            if (clones != null) {
                for (Iterator<OakIndexInput> it = clones.keyIterator(); it.hasNext();) {
                    final OakIndexInput clone = it.next();
                    assert clone.isClone;
                    clone.close();
                }
            }
        }

        private void checkNotClosed() {
            if (file.blob == null && file.data == null) {
                throw new AlreadyClosedException("Already closed: [" + dirDetails + "] " + this);
            }
        }

    }

    private final class OakIndexOutput extends IndexOutput {
        private final String dirDetails;
        private final OakIndexFile file;

        public OakIndexOutput(String name, NodeBuilder file, String dirDetails) throws IOException {
            this.dirDetails = dirDetails;
            this.file = new OakIndexFile(name, file, dirDetails);
        }

        @Override
        public long length() {
            return file.length;
        }

        @Override
        public long getFilePointer() {
            return file.position;
        }

        @Override
        public void seek(long pos) throws IOException {
            file.seek(pos);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length)
                throws IOException {
            try {
                file.writeBytes(b, offset, length);
            } catch (IOException e) {
                throw wrapWithDetails(e);
            }
        }

        @Override
        public void writeByte(byte b) throws IOException {
            writeBytes(new byte[] { b }, 0, 1);
        }

        @Override
        public void flush() throws IOException {
            try {
                file.flush();
            } catch (IOException e) {
                throw wrapWithDetails(e);
            }
        }

        @Override
        public void close() throws IOException {
            flush();
            file.blob = null;
            file.data = null;
        }

        private IOException wrapWithDetails(IOException e) {
            String msg = String.format("Error occurred while writing to blob [%s][%s]", dirDetails, file.getName());
            return new IOException(msg, e);
        }

    }

}
