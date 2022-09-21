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
package org.apache.jackrabbit.oak.commons;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * This class provides a common list of support compression algorithms and some utility functions.
 * It is mainly used by intermediate stored files in {@link org.apache.jackrabbit.oak.commons.sort.ExternalSort} and
 * sort/index utilities in {@link org.apache.jackrabbit.oak.index.indexer.document.flatfile}.
 */
public enum Compression {

    LZ4 {
        @Override
        public InputStream getInputStream(InputStream in) throws IOException {
            return new LZ4FrameInputStream(in);
        }
        @Override
        public OutputStream getOutputStream(OutputStream out) throws  IOException {
            return new LZ4FrameOutputStream(out);
        }
        @Override
        public String addSuffix(String filename) {
            return filename + ".lz4";
        }
    },
    GZIP {
        @Override
        public InputStream getInputStream(InputStream in) throws IOException {
            return new GZIPInputStream(in, 2048);
        }
        @Override
        public OutputStream getOutputStream(OutputStream out) throws  IOException {
            return new GZIPOutputStream(out, 2048) {
                {
                    def.setLevel(Deflater.BEST_SPEED);
                }
            };
        }
        @Override
        public String addSuffix(String filename) {
            return filename + ".gz";
        }
    },
    NONE,
    ;

    public InputStream getInputStream(InputStream in) throws IOException {
        return in;
    }

    public OutputStream getOutputStream(OutputStream out) throws  IOException {
        return out;
    }

    public String addSuffix(String filename) {
        return filename;
    }
}