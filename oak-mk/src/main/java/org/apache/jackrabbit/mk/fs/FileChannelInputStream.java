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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Allows to read from a file channel like an input stream.
 */
public class FileChannelInputStream extends InputStream {

    private final FileChannel channel;
    private final byte[] buffer = { 0 };
    private final boolean closeChannel;

    /**
     * Create a new file object input stream from the file channel.
     *
     * @param channel the file channel
     * @param closeChannel close the channel when done
     */
    public FileChannelInputStream(FileChannel channel, boolean closeChannel) {
        this.channel = channel;
        this.closeChannel = closeChannel;
    }

    @Override
    public int read() throws IOException {
        if (channel.position() >= channel.size()) {
            return -1;
        }
        FileUtils.readFully(channel, ByteBuffer.wrap(buffer));
        return buffer[0] & 0xff;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (channel.position() + len < channel.size()) {
            FileUtils.readFully(channel, ByteBuffer.wrap(b, off, len));
            return len;
        }
        return super.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        n = Math.min(channel.size() - channel.position(), n);
        channel.position(channel.position() + n);
        return n;
    }

    @Override
    public void close() throws IOException {
        if (closeChannel) {
            channel.close();
        }
    }

}
