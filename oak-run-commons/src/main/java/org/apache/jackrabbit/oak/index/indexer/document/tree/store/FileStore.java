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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStore implements Store {
    private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

    private final Properties config;
    private final String directory;
    private Compression compression = Compression.NO;
    private long writeCount, readCount;
    private Thread backgroundThread;
    private final ConcurrentHashMap<String, PageFile> pendingWrites = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<WriteOperation> queue = new LinkedBlockingQueue<>(100);

    private static final WriteOperation STOP = new WriteOperation();

    static class WriteOperation {
        String key;
        byte[] value;
    }

    public String toString() {
        return "file(" + directory + ")";
    }

    public FileStore(Properties config) {
        this.config = config;
        this.directory = config.getProperty("dir");
        if (this.directory == null) {
            throw new IllegalArgumentException("dir must be set");
        }
        boolean dirsCreated = new File(directory).mkdirs();
        if (!dirsCreated) {
            LOG.warn("Directory already exists: {}", directory);
        }
        boolean asyncWrite = Boolean.parseBoolean(config.getProperty("async", "false"));
        if (asyncWrite) {
            startAsyncWriter();
        }
    }

    private void startAsyncWriter() {
        backgroundThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    for (int i = 0;; i++) {
                        WriteOperation op = queue.take();
                        if (i % 200 == 0) {
                            // System.out.println("  file writer queue size " + queue.size());
                        }
                        if (op == STOP) {
                            break;
                        }
                        writeFile(op.key, op.value);
                        pendingWrites.remove(op.key);
                    }
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }

        });
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }

    @Override
    public void close() {
        try {
            if (backgroundThread != null) {
                queue.put(STOP);
                backgroundThread.join();
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void setWriteCompression(Compression compression) {
        this.compression = compression;
    }

    @Override
    public PageFile getIfExists(String key) {
        PageFile pending = pendingWrites.get(key);
        if (pending != null) {
            return pending;
        }
        readCount++;
        File f = getFile(key);
        if (!f.exists()) {
            LOG.info("File does not exist: {}", f);
            return null;
        }
        try (RandomAccessFile file = new RandomAccessFile(f, "r")) {
            long length = file.length();
            if (length == 0) {
                LOG.info("File is empty: {}", f);
                // deleted in the meantime
                return null;
            }
            // TODO: decompress while streaming the data from the disk. Avoiding loading the whole file in memory
            //  before decompressing. Even better, create a PageFile.fromStream(InputStream) method and pass it a
            //  stream that reads and decompresses. This will cut the amount of intermediate memory buffers needed.
            byte[] data = new byte[(int) length];
            file.readFully(data);
            Compression c = Compression.getCompressionFromData(data[0]);
            data = c.expand(data);
            return PageFile.fromBytes(data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void put(String key, PageFile value) {
        writeCount++;
        if (backgroundThread != null) {
            writeFileAsync(key, value.copy());
        } else {
            writeFile(key, value.toBytes());
        }
    }

    @Override
    public boolean putIfAbsent(String key, PageFile value) {
        writeCount++;
        File file = getFile(key);
        byte[] data = value.toBytes();
        Path path = Paths.get(file.getAbsolutePath());
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(data));
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void writeFileAsync(String key, PageFile value) {
        pendingWrites.put(key, value);
        WriteOperation op = new WriteOperation();
        op.key = key;
        op.value = value.toBytes();
        try {
            queue.put(op);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public boolean supportsByteOperations() {
        return true;
    }

    @Override
    public byte[] getBytes(String key) {
        File f = getFile(key);
        if (!f.exists()) {
            return null;
        }
        try {
            readCount++;
            try (RandomAccessFile file = new RandomAccessFile(f, "r")) {
                long length = file.length();
                if (length == 0) {
                    // deleted in the meantime
                    return null;
                }
                byte[] data = new byte[(int) length];
                file.readFully(data);
                return data;
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void putBytes(String key, byte[] data) {
        try (FileOutputStream out = new FileOutputStream(getFile(key))) {
            out.write(data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void writeFile(String key, byte[] data) {
        data = compression.compress(data);
        putBytes(key, data);
    }

    private File getFile(String key) {
        return new File(directory, key);
    }

    @Override
    public String newFileName() {
        return Uuid.timeBasedVersion7().toShortString();
    }

    @Override
    public Set<String> keySet() {
        File dir = new File(directory);
        if (!dir.exists()) {
            return Collections.emptySet();
        }
        String[] list = dir.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isFile();
            }

        });
        if (list == null) {
            throw new IllegalStateException("Failed to list files in " + directory);
        }
        return new HashSet<>(Arrays.asList(list));
    }

    @Override
    public void remove(Set<String> set) {
        // TODO keep for some time if the file is relatively new?
        for (String key : set) {
            writeCount++;
            boolean deleted = getFile(key).delete();
            if (!deleted) {
                LOG.debug("Failed to delete file {}", key);
            }
        }
    }

    @Override
    public void removeAll() {
        File dir = new File(directory);
        for(File f: dir.listFiles()) {
            boolean deleted = f.delete();
            if (!deleted) {
                LOG.debug("Failed to delete file {}", f.getName());
            }
        }
    }

    @Override
    public long getWriteCount() {
        return writeCount;
    }

    @Override
    public long getReadCount() {
        return readCount;
    }

    @Override
    public Properties getConfig() {
        return config;
    }

}
