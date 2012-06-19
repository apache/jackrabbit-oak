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
package org.apache.jackrabbit.mk.server;

import java.io.InputStream;
import java.io.IOException;

/**
 * Stream that reads bytes until it sees a given string boundary, preceded
 * by CR+LF, as used in multipart/form-data uploads.
 */
class BoundaryInputStream extends InputStream {

    private InputStream in;

    private final byte[] boundary;

    private final byte[] buf;

    private int offset;

    private int count;

    private int boundaryIndex;

    private boolean eos;

    /**
     * Create a new instance of this class.
     *
     * @param in base input
     * @param boundary boundary
     */
    public BoundaryInputStream(InputStream in, String boundary) {
        this(in, boundary, 8192);
    }

    /**
     * Create a new instance of this class.
     *
     * @param in base input
     * @param boundary boundary
     * @param size size of internal read-ahead buffer
     */
    public BoundaryInputStream(InputStream in, String boundary, int size) {
        this.in = in;
        this.boundary = ("\r\n" + boundary).getBytes();

        // Must be able to unread this many bytes
        if (size < this.boundary.length + 2) {
            size = this.boundary.length + 2;
        }
        buf = new byte[size];
    }

    @Override
    public int read() throws IOException {
        if (eos) {
            return -1;
        }
        byte[] b = new byte[1];
        int count = read(b, 0, 1);
        if (count == -1) {
            return -1;
        }
        return b[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (eos) {
            return -1;
        }
        if (offset == count) {
            fillBuffer();
            if (eos) {
                return -1;
            }
        }
        return copy(b, off, len);
    }

    private void fillBuffer() throws IOException {
        if (boundaryIndex > 0) {
            System.arraycopy(boundary, 0, buf, 0, boundaryIndex);
        }
        offset = boundaryIndex;
        count = in.read(buf, offset, buf.length - offset);

        if (count < 0) {
            eos = true;
        }
        count += offset;
    }

    private int copy(byte[] b, int off, int len) throws IOException {
        int i = 0, j = 0;

        while (offset + i < count && j < len) {
            if (boundary[boundaryIndex] == buf[offset + i]) {
                boundaryIndex++;
                i++;

                if (boundaryIndex == boundary.length) {
                    eos = true;
                    break;
                }
            } else {
                if (boundaryIndex > 0) {
                    i -= boundaryIndex;
                    if (i < 0) {
                        offset += i;
                        i = 0;
                    }
                    boundaryIndex = 0;
                }
                b[off + j] = buf[offset + i];
                i++;
                j++;
            }
        }
        offset += i;
        return j == 0 && eos ? -1 : j;
    }

    @Override
    public void close() throws IOException {
        in = null;
        eos = true;
    }
}
