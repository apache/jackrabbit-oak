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
package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.LZ4Compression;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedOutputStream;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class IndexStoreUtils {
    public static final String METADATA_SUFFIX = ".metadata";

    public static final String OAK_INDEXER_USE_ZIP = "oak.indexer.useZip";
    public static final String OAK_INDEXER_USE_LZ4 = "oak.indexer.useLZ4";

    public static boolean compressionEnabled() {
        return Boolean.parseBoolean(System.getProperty(OAK_INDEXER_USE_ZIP, "true"));
    }

    public static boolean useLZ4() {
        return Boolean.parseBoolean(System.getProperty(OAK_INDEXER_USE_LZ4, "true"));
    }

    public static Compression compressionAlgorithm() {
        if (!compressionEnabled()) {
            return Compression.NONE;
        }
        return useLZ4() ? new LZ4Compression() : Compression.GZIP;
    }

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

    public static OutputStream createOutputStream(Path file, Compression algorithm) throws IOException {
        // The output streams created by LZ4 and GZIP buffer their input, so we should not wrap then again.
        // However, the implementation of the compression streams may make small writes to the underlying stream,
        // so we buffer the FileOutputStream
        OutputStream out = new BufferedOutputStream(Files.newOutputStream(file));
        return algorithm.getOutputStream(out);
    }

    public static long sizeOf(List<File> sortedFiles) {
        return sortedFiles.stream().mapToLong(File::length).sum();
    }

    public static String getSortedStoreFileName(Compression algorithm) {
        return algorithm.addSuffix("store-sorted.json");
    }

    public static String getMetadataFileName(Compression algorithm) {
        return algorithm.addSuffix("store-sorted.json.metadata");
    }

    /*
        Metadata file is placed in same folder as IndexStore file with following naming convention.
        e.g. <filename.json>.<compression suffix> then metadata file is stored as <filename.json>.metadata.<compression suffix>
     */
    public static File getMetadataFile(File indexStoreFile, Compression algorithm) {
        File metadataFile;
        if (algorithm.equals(Compression.NONE)) {
            metadataFile = new File(indexStoreFile.getAbsolutePath() + METADATA_SUFFIX);
        } else {
            String fileName = indexStoreFile.getName();
            String compressionSuffix = getCompressionSuffix(indexStoreFile);
            Validate.checkState(algorithm.addSuffix("").equals(compressionSuffix));
            String fileNameWithoutCompressionSuffix = fileName.substring(0, fileName.lastIndexOf("."));
            metadataFile = new File(algorithm.addSuffix(indexStoreFile.getParent() + "/"
                    + fileNameWithoutCompressionSuffix + METADATA_SUFFIX));
        }
        return metadataFile;
    }

    private static String getCompressionSuffix(File file) {
        int lastDot = file.getName().lastIndexOf(".");
        if (lastDot < 0) {
            return "";
        }
        return file.getName().substring(lastDot);
    }

    /**
     * This method validates the compression suffix is in correspondence with compression algorithm.
     */
    public static void validateFlatFileStoreFileName(File file, @NotNull Compression algorithm) {
        if (!algorithm.equals(Compression.NONE)) {
            Validate.checkState(algorithm.addSuffix("")
                            .equals(getCompressionSuffix(file)),
                    "File suffix should be in correspondence with compression algorithm. Filename:{}, Compression suffix:{} ",
                    file.getAbsolutePath(), algorithm.addSuffix(""));
        }
    }

}
