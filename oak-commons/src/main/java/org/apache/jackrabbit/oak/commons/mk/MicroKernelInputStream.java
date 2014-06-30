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
package org.apache.jackrabbit.oak.commons.mk;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.commons.IOUtils;

/**
 * An input stream to simplify reading a blob from a {@code MicroKernel}.
 */
public class MicroKernelInputStream extends InputStream {

    private final MicroKernel mk;
    private final String id;
    private long pos;
    private long length = -1;
    private byte[] oneByteBuff;

    public MicroKernelInputStream(MicroKernel mk, String id) {
        this.mk = mk;
        this.id = id;
    }

    @Override
    public long skip(long n) {
        if (n < 0) {
            return 0;
        }
        if (length == -1) {
            length = mk.getLength(id);
        }
        n = Math.min(n, length - pos);
        pos += n;
        return n;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        int l = mk.read(id, pos, b, off, len);
        if (l < 0) {
            return l;
        }
        pos += l;
        return l;
    }

    @Override
    public int read() throws IOException {
        if (oneByteBuff == null) {
            oneByteBuff = new byte[1];
        }
        int len = read(oneByteBuff, 0, 1);
        if (len < 0) {
            return len;
        }
        return oneByteBuff[0] & 0xff;
    }

    public static byte[] readFully(MicroKernel mk, String id) throws IOException {
        int len = (int) mk.getLength(id);
        byte[] buff = new byte[len];
        MicroKernelInputStream in = new MicroKernelInputStream(mk, id);
        IOUtils.readFully(in, buff, 0, len);
        return buff;
    }

}
