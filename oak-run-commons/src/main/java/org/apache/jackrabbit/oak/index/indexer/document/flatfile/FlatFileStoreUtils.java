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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Charsets.UTF_8;

class FlatFileStoreUtils {

    public static BufferedReader createReader(File file, boolean compressionEnabled) {
        try {
            BufferedReader br;
            InputStream in = new FileInputStream(file);
            if (compressionEnabled) {
                br = new BufferedReader(new InputStreamReader(new GZIPInputStream(in, 2048), UTF_8));
            } else {
                br = new BufferedReader(new InputStreamReader(in, UTF_8));
            }
            return br;
        } catch (IOException e) {
            throw new RuntimeException("Error opening file " + file, e);
        }
    }

    public static BufferedWriter createWriter(File file, boolean compressionEnabled) throws IOException {
        OutputStream out = new FileOutputStream(file);
        if (compressionEnabled) {
            out = new GZIPOutputStream(out, 2048) {
                {
                    def.setLevel(Deflater.BEST_SPEED);
                }
            };
        }
        return new BufferedWriter(new OutputStreamWriter(out, UTF_8));
    }

    public static long sizeOf(List<File> sortedFiles) {
        return sortedFiles.stream().mapToLong(File::length).sum();
    }

    public static String getSortedStoreFileName(boolean compressionEnabled){
        return compressionEnabled ? "store-sorted.json.gz" : "store-sorted.json";
    }
}
