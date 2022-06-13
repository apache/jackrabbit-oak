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

import org.apache.jackrabbit.oak.commons.sort.ExternalSort;

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

class FlatFileStoreUtils {

    public static final String COMPRESSION_TYPE_LZ4 = "lz4";
    public static final String COMPRESSION_TYPE_GZIP = "gz";
    public static final String COMPRESSION_TYPE_NONE = "none";

    /**
     * A map of compression Type to ExternalSort CompressionType Enum
     * @param compressionType string representation of the compression algorithm, use "none" for disable compression,
     *                        any unknown or unsupported value will be considered as none.
     * @return
     */
    public static ExternalSort.CompressionType getExternalSortCompressionType(String compressionType) {
        switch (compressionType) {
            case COMPRESSION_TYPE_LZ4:
                return ExternalSort.CompressionType.LZ4;
            case COMPRESSION_TYPE_GZIP:
                return ExternalSort.CompressionType.GZIP;
            case COMPRESSION_TYPE_NONE:
                return ExternalSort.CompressionType.NONE;
            default:
                return ExternalSort.CompressionType.NONE;
        }
    }

    public static BufferedReader createReader(File file, boolean compressionEnabled) {
        return createReader(file, compressionEnabled ? COMPRESSION_TYPE_GZIP : COMPRESSION_TYPE_NONE);
    }

    public static BufferedReader createReader(File file, String compressionType) {
        try {
            InputStream in = new FileInputStream(file);
            return new BufferedReader(new InputStreamReader(getExternalSortCompressionType(compressionType).getInputStream(in)));
        } catch (IOException e) {
            throw new RuntimeException("Error opening file " + file, e);
        }
    }

    public static BufferedWriter createWriter(File file, boolean compressionEnabled) throws IOException {
        return createWriter(file, compressionEnabled ? COMPRESSION_TYPE_GZIP : COMPRESSION_TYPE_NONE);
    }

    public static BufferedWriter createWriter(File file, String compressionType) throws IOException {
        OutputStream out = new FileOutputStream(file);
        return new BufferedWriter(new OutputStreamWriter(getExternalSortCompressionType(compressionType).getOutputStream(out)));
    }

    public static long sizeOf(List<File> sortedFiles) {
        return sortedFiles.stream().mapToLong(File::length).sum();
    }

    public static String getSortedStoreFileName(boolean compressionEnabled) {
        return getSortedStoreFileName(compressionEnabled ? COMPRESSION_TYPE_GZIP : COMPRESSION_TYPE_NONE);
    }

    public static String getSortedStoreFileName(String compressionType) {
        String name = "store-sorted.json";
        if (compressionType.equals(COMPRESSION_TYPE_NONE)) {
            return name;
        }
        return name + "." + compressionType;
    }
}
