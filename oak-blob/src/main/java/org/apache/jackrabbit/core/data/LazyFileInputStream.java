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
package org.apache.jackrabbit.core.data;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.input.AutoCloseInputStream;

/**
 * This input stream delays opening the file until the first byte is read, and
 * closes and discards the underlying stream as soon as the end of input has
 * been reached or when the stream is explicitly closed.
 */
public class LazyFileInputStream extends AutoCloseInputStream {

    /**
     * The file descriptor to use.
     */
    protected final FileDescriptor fd;

    /**
     * The file to read from.
     */
    protected final File file;

    /**
     * True if the input stream was opened. It is also set to true if the stream
     * was closed without reading (to avoid opening the file after the stream
     * was closed).
     */
    protected boolean opened;

    /**
     * Creates a new <code>LazyFileInputStream</code> for the given file. If the
     * file is unreadable, a FileNotFoundException is thrown.
     * The file is not opened until the first byte is read from the stream.
     *
     * @param file the file
     * @throws java.io.FileNotFoundException
     */
    public LazyFileInputStream(File file)
            throws FileNotFoundException {
        super(null);
        if (!file.canRead()) {
            throw new FileNotFoundException(file.getPath());
        }
        this.file = file;
        this.fd = null;
    }

    /**
     * Creates a new <code>LazyFileInputStream</code> for the given file
     * descriptor.
     * The file is not opened until the first byte is read from the stream.
     *
     * @param fd
     */
    public LazyFileInputStream(FileDescriptor fd) {
        super(null);
        this.file = null;
        this.fd = fd;
    }

    /**
     * Creates a new <code>LazyFileInputStream</code> for the given file. If the
     * file is unreadable, a FileNotFoundException is thrown.
     *
     * @param name
     * @throws java.io.FileNotFoundException
     */
    public LazyFileInputStream(String name) throws FileNotFoundException {
        this(new File(name));
    }

    /**
     * Open the stream if required.
     *
     * @throws java.io.IOException
     */
    protected void open() throws IOException {
        if (!opened) {
            opened = true;
            if (fd != null) {
                in = new FileInputStream(fd);
            } else {
                in = new FileInputStream(file);
            }
        }
    }

    public int read() throws IOException {
        open();
        return super.read();
    }

    public int available() throws IOException {
        open();
        return super.available();
    }

    public void close() throws IOException {
        // make sure the file is not opened afterwards
        opened = true;
        
        // only close the file if it was in fact opened
        if (in != null) {
            super.close();
        }
    }

    public synchronized void reset() throws IOException {
        open();
        super.reset();
    }

    public boolean markSupported() {
        try {
            open();
        } catch (IOException e) {
            throw new IllegalStateException(e.toString());
        }
        return super.markSupported();
    }

    public synchronized void mark(int readlimit) {
        try {
            open();
        } catch (IOException e) {
            throw new IllegalStateException(e.toString());
        }
        super.mark(readlimit);
    }

    public long skip(long n) throws IOException {
        open();
        return super.skip(n);
    }

    public int read(byte[] b) throws IOException {
        open();
        return super.read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        open();
        return super.read(b, off, len);
    }

}
