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
package org.apache.jackrabbit.oak.plugins.memory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.jackrabbit.oak.commons.Compression;
import org.jetbrains.annotations.NotNull;

/**
 * This {@code Blob} implementations is based on an array of bytes.
 */
public class ArrayBasedBlob extends AbstractBlob {
    private final byte[] value;
    private final long valueLength;

    private Compression compression = Compression.GZIP;

    public ArrayBasedBlob(byte[] value) {
        System.out.println("value = " + value.length);
        this.value = compress(value);
        System.out.println("value = " + this.value.length);
        this.valueLength = value.length;
    }

    private byte[] compress(byte[] value) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            OutputStream compressionOutputStream = compression.getOutputStream(out);
            compressionOutputStream.write(value);
            compressionOutputStream.close();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress data", e);
        }
    }

    @NotNull
    @Override
    public InputStream getNewStream() {
        try {
            return compression.getInputStream(new ByteArrayInputStream(this.value));
        } catch (IOException e) {
            throw new RuntimeException("Failed to decompress data", e);
        }
    }

    @Override
    public long length() {
        return this.valueLength;
    }
}
