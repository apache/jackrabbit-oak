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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class that allows converting the files of a tree store into one
 * file (pack the files), and back from a file to a list of files (unpack the
 * files). This is a bit similar to a zip file, however
 *
 * - each entry is already compressed, so no additional compression is needed;
 * - only files in the same directory can be processed;
 * - the pack file starts with a header that contains the list of files;
 * - while packing, the files are (optionally) deleted, so that this doesn't require twice the disk space;
 * - while unpacking, the pack file is (optionally) truncated, also to conserve disk space.
 */
public class FilePacker {

    private static final Logger LOG = LoggerFactory.getLogger(FilePacker.class);

    /**
     * The header of pack files ("PACK").
     */
    public static final String PACK_HEADER = "PACK";

    public static void main(String... args) throws IOException {
        if (args.length <= 2) {
            System.out.println("Usage:\n" +
                    "  java -jar target/oak-run-commons-*.jar " +
                    FilePacker.class.getCanonicalName() + " -d <fileName>\n" +
                    "  expands a file into the contained files");
            return;
        }
        if (args[0].equals("-d")) {
            File packFile = new File(args[1]);
            unpack(packFile, packFile.getParentFile(), false);
        }
    }

    /**
     * Check whether the file starts with the magic header.
     *
     * @param file
     * @return if this is a pack file
     */
    public static boolean isPackFile(File file) {
        if (!file.exists() || !file.isFile() || file.length() < 4) {
            return false;
        }
        try (RandomAccessFile source = new RandomAccessFile(file, "r")) {
            byte[] magic = new byte[4];
            source.readFully(magic);
            return PACK_HEADER.equals(new String(magic, StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOG.warn("IOException reading a possible pack file", e);
            return false;
        }
    }

    /**
     * Packs all the files in the source directory into a target file.
     *
     * @param sourceDirectory the source directory
     * @param fileNameRegex the file name regular expression
     * @param deleteSource whether the source files are deleted while copying
     */
    public static void pack(File sourceDirectory, String fileNameRegex, File targetFile, boolean deleteSource) throws IOException {
        if (!sourceDirectory.exists() || !sourceDirectory.isDirectory()) {
            throw new IOException("Source directory doesn't exist or is a file: " + sourceDirectory.getAbsolutePath());
        }
        List<FileEntry> list = Files.list(sourceDirectory.toPath()).
                map(p -> p.toFile()).
                filter(f -> f.isFile()).
                filter(f -> f.getName().matches(fileNameRegex)).
                map(f -> new FileEntry(f)).
                collect(Collectors.toList());
        RandomAccessFile target = new RandomAccessFile(targetFile, "rw");
        target.write(PACK_HEADER.getBytes(StandardCharsets.UTF_8));
        for (FileEntry f : list) {
            target.write(1);
            byte[] name = f.fileName.getBytes(StandardCharsets.UTF_8);
            target.writeInt(name.length);
            target.write(name);
            target.writeLong(f.length);
        }
        target.write(0);
        for (FileEntry f : list) {
            File file = new File(sourceDirectory, f.fileName);
            FileInputStream in = new FileInputStream(file);
            in.getChannel().transferTo(0, f.length, target.getChannel());
            in.close();
            if (deleteSource) {
                // delete after copying to avoid using twice the space
                file.delete();
            }
        }
        target.close();
    }

    /**
     * Unpack a target file target file. The target directory is created if needed.
     * Existing files are overwritten.
     *
     * @param sourceFile      the pack file
     * @param targetDirectory the target directory
     * @param deleteSource    whether the source file is truncated while copying,
     *                        and finally deleted.
     */
    public static void unpack(File sourceFile, File targetDirectory, boolean deleteSource) throws IOException {
        if (!sourceFile.exists() || !sourceFile.isFile()) {
            throw new IOException("Source file doesn't exist or is not a file: " + sourceFile.getAbsolutePath());
        }
        if (targetDirectory.exists()) {
            if (targetDirectory.isFile()) {
                throw new IOException("Target file exists: " + targetDirectory.getAbsolutePath());
            }
        } else {
            targetDirectory.mkdirs();
        }
        RandomAccessFile source = new RandomAccessFile(sourceFile, "rw");
        ArrayList<FileEntry> list = readDirectoryListing(sourceFile, source);
        long start = source.getFilePointer();
        for (int i = list.size() - 1; i >= 0; i--) {
            FileEntry f = list.get(i);
            source.seek(start + f.offset);
            FileOutputStream out = new FileOutputStream(new File(targetDirectory, f.fileName));
            source.getChannel().transferTo(source.getFilePointer(), f.length, out.getChannel());
            out.close();
            if (deleteSource) {
                // truncate the source to avoid using twice the space
                source.setLength(start + f.offset);
            }
        }
        source.close();
        if (deleteSource) {
            sourceFile.delete();
        }
    }

    public static ArrayList<FileEntry> readDirectoryListing(File sourceFile, RandomAccessFile source) throws IOException {
        byte[] magic = new byte[4];
        source.readFully(magic);
        if (!PACK_HEADER.equals(new String(magic, StandardCharsets.UTF_8))) {
            source.close();
            throw new IOException("File header is not '" + PACK_HEADER + "': " + sourceFile.getAbsolutePath());
        }
        ArrayList<FileEntry> list = new ArrayList<>();
        long offset = 0;
        while (source.read() > 0) {
            byte[] name = new byte[source.readInt()];
            source.readFully(name);
            long length = source.readLong();
            list.add(new FileEntry(new String(name, StandardCharsets.UTF_8), length, offset));
            offset += length;
        }
        return list;
    }

    public static class FileEntry {
        public final String fileName;
        public final long length;
        public long offset;
        FileEntry(String fileName, long length, long offset) {
            this.fileName = fileName;
            this.length = length;
            this.offset = offset;
        }
        FileEntry(File f) {
            this.fileName = f.getName();
            this.length = f.length();
        }
        public String toString() {
            return fileName + "/" + length + "@" + offset;
        }
    }
}
