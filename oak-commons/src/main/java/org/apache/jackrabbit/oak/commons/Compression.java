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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * This interface provides a default list of support compression algorithms and some utility functions.
 * It is mainly used by intermediate stored files in {@link org.apache.jackrabbit.oak.commons.sort.ExternalSort} and
 * sort/index utilities in {@link org.apache.jackrabbit.oak.index.indexer.document.flatfile}.
 *
 * Other compression algorithms can be supported by implementing the methods.
 */
public interface Compression {
    Compression NONE = new Compression() {
        public InputStream getInputStream(InputStream in) throws IOException {
            return in;
        }

        public OutputStream getOutputStream(OutputStream out) throws  IOException {
            return out;
        }

        public String addSuffix(String filename) {
            return filename;
        }
    };

    Compression GZIP = new Compression() {
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
    };

    InputStream getInputStream(InputStream in) throws IOException;
    OutputStream getOutputStream(OutputStream out) throws  IOException ;
    String addSuffix(String filename);
}