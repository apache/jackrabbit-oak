/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.jackrabbit.mk.util.SimpleLRUCache;

/**
 * A file that has a simple read cache.
 */
public class FileCache extends FileBase {

    private static final boolean APPEND_BUFFER = !Boolean.getBoolean("mk.disableAppendBuffer");
    private static final int APPEND_BUFFER_SIZE_INIT = 8 * 1024;
    private static final int APPEND_BUFFER_SIZE = 8 * 1024;

    private static final int BLOCK_SIZE = 4 * 1024;
    private final String name;
    private final Map<Long, ByteBuffer> readCache = SimpleLRUCache.newInstance(16);
    private final FileChannel base;
    private long pos, size;

    private AtomicReference<ByteArrayOutputStream> appendBuffer;
    private int appendOperations;
    private Thread appendFlushThread;

    FileCache(String name, FileChannel base) throws IOException {
        this.name = name;
        this.base = base;
        this.size = base.size();
    }

    @Override
    public long position() throws IOException {
        return pos;
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        this.pos = newPosition;
        return this;
    }

    boolean flush() throws IOException {
        if (appendBuffer == null) {
            return false;
        }
        synchronized (this) {
            ByteArrayOutputStream newBuff = new ByteArrayOutputStream(APPEND_BUFFER_SIZE_INIT);
            ByteArrayOutputStream buff = appendBuffer.getAndSet(newBuff);
            if (buff.size() > 0) {
                try {
                    base.position(size - buff.size());
                    base.write(ByteBuffer.wrap(buff.toByteArray()));
                } catch (IOException e) {
                    close();
                    throw e;
                }
            }
        }
        return true;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        flush();
        long readPos = (pos / BLOCK_SIZE) * BLOCK_SIZE;
        int off = (int) (pos - readPos);
        int len = BLOCK_SIZE - off;
        ByteBuffer buff = readCache.get(readPos);
        if (buff == null) {
            base.position(readPos);
            buff = ByteBuffer.allocate(BLOCK_SIZE);
            int read = base.read(buff);
            if (read == BLOCK_SIZE) {
                readCache.put(readPos, buff);
            } else {
                if (read < 0) {
                    return -1;
                }
                len = Math.min(len, read);
            }
        }
        len = Math.min(len, dst.remaining());
        System.arraycopy(buff.array(), off, dst.array(), dst.position(), len);
        dst.position(dst.position() + len);
        pos += len;
        return len;
    }

    @Override
    public long size() throws IOException {
        return size;
    }

    @Override
    public FileChannel truncate(long newSize) throws IOException {
        flush();
        readCache.clear();
        base.truncate(newSize);
        pos = Math.min(pos, newSize);
        size = Math.min(size, newSize);
        return this;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (readCache.size() > 0) {
            readCache.clear();
        }
        // append operations are buffered, but
        // only if there was at least one successful write operation
        // (to detect trying to write to a read-only file and such early on)
        // (in addition to that, the first few append operations are not buffered
        // to avoid starting a thread unnecessarily)
        if (APPEND_BUFFER && pos == size && ++appendOperations >= 4) {
            int len = src.remaining();
            if (len > APPEND_BUFFER_SIZE) {
                flush();
            } else {
                if (appendBuffer == null) {
                    ByteArrayOutputStream buff = new ByteArrayOutputStream(APPEND_BUFFER_SIZE_INIT);
                    appendBuffer = new AtomicReference<ByteArrayOutputStream>(buff);
                    appendFlushThread = new Thread("Flush " + name) {
                        @Override
                        public void run() {
                            try {
                                do {
                                    Thread.sleep(500);
                                    if (flush()) {
                                        continue;
                                    }
                                } while (!Thread.interrupted());
                            } catch (Exception e) {
                                // ignore
                            }
                        }
                    };
                    appendFlushThread.setDaemon(true);
                    appendFlushThread.start();
                }
                ByteArrayOutputStream buff = appendBuffer.get();
                if (buff.size() > APPEND_BUFFER_SIZE) {
                    flush();
                    buff = appendBuffer.get();
                }
                buff.write(src.array(), src.position(), len);
                pos += len;
                size += len;
                return len;
            }
        }
        base.position(pos);
        int len = base.write(src);
        pos += len;
        size = Math.max(size, pos);
        return len;
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (appendBuffer != null) {
            appendFlushThread.interrupt();
            try {
                appendFlushThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            flush();
        }
        base.close();
    }

    @Override
    public void force(boolean metaData) throws IOException {
        flush();
        base.force(metaData);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        flush();
        return base.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return "cache:" + base.toString();
    }

}