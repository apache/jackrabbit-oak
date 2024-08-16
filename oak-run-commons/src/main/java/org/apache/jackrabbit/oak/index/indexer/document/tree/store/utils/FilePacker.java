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

public class FilePacker {

    /**
     * Packs all the files in the source directory into a target file.
     * The file header is "PACK".
     *
     * @param deleteSource whether the source files are deleted while copying
     */
    public static void pack(File sourceDirectory, File targetFile, boolean deleteSource) throws IOException {
        if (!sourceDirectory.exists() || !sourceDirectory.isDirectory()) {
            throw new IOException("Source directory doesn't exist or is a file: " + sourceDirectory.getAbsolutePath());
        }
        List<FileEntry> list = Files.list(sourceDirectory.toPath()).
                map(p -> p.toFile()).
                filter(f -> f.isFile()).
                map(f -> new FileEntry(f)).
                collect(Collectors.toList());
        RandomAccessFile target = new RandomAccessFile(targetFile, "rw");
        target.writeUTF("PACK");
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
        if (!"PACK".equals(source.readUTF())) {
            source.close();
            throw new IOException("File header is not 'PACK': " + sourceFile.getAbsolutePath());
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

    static class FileEntry {
        final String fileName;
        final long length;
        long offset;
        FileEntry(String fileName, long length, long offset) {
            this.fileName = fileName;
            this.length = length;
            this.offset = offset;
        }
        FileEntry(File f) {
            this.fileName = f.getName();
            this.length = f.length();
        }
    }
}
