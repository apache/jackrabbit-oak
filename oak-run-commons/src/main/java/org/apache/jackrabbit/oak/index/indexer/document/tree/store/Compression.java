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

import java.util.Arrays;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * The enum allows to disable or enable compression of the storage files.
 */
public enum Compression {
    NO {
        @Override
        byte[] compress(byte[] data) {
            return data;
        }

        @Override
        byte[] expand(byte[] data) {
            return data;
        }
    },
    LZ4 {
        private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        private final LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

        private byte[] compressBuffer = new byte[1024 * 1024];

        @Override
        byte[] compress(byte[] data) {
            // synchronization is needed because we share the buffer
            synchronized (compressor) {
                byte[] buffer = compressBuffer;
                if (buffer.length < 2 * data.length) {
                    // increase the size
                    buffer = new byte[2 * data.length];
                    compressBuffer = buffer;
                }
                buffer[0] = '4';
                writeInt(buffer, 1, data.length);
                int len = 5 + compressor.compress(data, 0, data.length, buffer, 5, buffer.length - 5);
                return Arrays.copyOf(buffer, len);
            }
        }

        @Override
        byte[] expand(byte[] data) {
            int len = readInt(data, 1);
            byte[] target = new byte[len];
            decompressor.decompress(data, 5, target, 0, len);
            return target;
        }
    };

    abstract byte[] compress(byte[] data);
    abstract byte[] expand(byte[] data);


    public static void writeInt(byte[] buff, int pos, int x) {
        buff[pos++] = (byte) (x >> 24);
        buff[pos++] = (byte) (x >> 16);
        buff[pos++] = (byte) (x >> 8);
        buff[pos] = (byte) x;
    }

    public static int readInt(byte[] buff, int pos) {
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8) + (buff[pos] & 0xff);
    }

    public static Compression getCompressionFromData(byte data) {
        switch (data) {
        case '4':
            return Compression.LZ4;
        default:
            return Compression.NO;
        }
    }

}