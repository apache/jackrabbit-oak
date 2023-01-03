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

import org.apache.jackrabbit.oak.commons.Compression;

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

/**
 * This class provides common utility functions for building FlatFileStore.
 */
class FlatFileStoreUtils {

    /**
     * This function by default uses GNU zip as compression algorithm for backward compatibility.
     */
    public static BufferedReader createReader(File file, boolean compressionEnabled) {
        return createReader(file, compressionEnabled ? Compression.GZIP : Compression.NONE);
    }

    public static BufferedReader createReader(File file, Compression algorithm) {
        try {
            InputStream in = new FileInputStream(file);
            return new BufferedReader(new InputStreamReader(algorithm.getInputStream(in)));
        } catch (IOException e) {
            throw new RuntimeException("Error opening file " + file, e);
        }
    }

    /**
     * This function by default uses GNU zip as compression algorithm for backward compatibility.
     */
    public static BufferedWriter createWriter(File file, boolean compressionEnabled) throws IOException {
        return createWriter(file, compressionEnabled ? Compression.GZIP : Compression.NONE);
    }

    public static BufferedWriter createWriter(File file, Compression algorithm) throws IOException {
        OutputStream out = new FileOutputStream(file);
        return new BufferedWriter(new OutputStreamWriter(algorithm.getOutputStream(out)));
    }

    public static long sizeOf(List<File> sortedFiles) {
        return sortedFiles.stream().mapToLong(File::length).sum();
    }

    /**
     * This function by default uses GNU zip as compression algorithm for backward compatibility.
     */
    public static String getSortedStoreFileName(boolean compressionEnabled) {
        return getSortedStoreFileName(compressionEnabled ? Compression.GZIP : Compression.NONE);
    }

    public static String getSortedStoreFileName(Compression algorithm) {
        return algorithm.addSuffix("store-sorted.json");
    }
}
