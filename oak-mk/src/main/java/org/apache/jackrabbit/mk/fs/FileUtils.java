/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.fs;

import org.apache.jackrabbit.mk.util.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * This utility class contains utility functions that use the file system
 * abstraction.
 */
public class FileUtils {

    private FileUtils() {
    }

    /**
     * Checks if a file exists.
     * This method is similar to Java 7 {@code java.nio.file.Path.exists}.
     *
     * @param fileName the file name
     * @return true if it exists
     */
    public static boolean exists(String fileName) {
        return FilePath.get(fileName).exists();
    }

    /**
     * Create a directory (all required parent directories must already exist).
     * This method is similar to Java 7 {@code java.nio.file.Path.createDirectory}.
     *
     * @param directoryName the directory name
     */
    public static void createDirectory(String directoryName) throws IOException {
        FilePath.get(directoryName).createDirectory();
    }

    /**
     * Create a new file.
     * This method is similar to Java 7 {@code java.nio.file.Path.createFile}, but returns
     * false instead of throwing a exception if the file already existed.
     *
     * @param fileName the file name
     * @return true if creating was successful
     */
    public static boolean createFile(String fileName) {
        return FilePath.get(fileName).createFile();
    }

    /**
     * Delete a file or directory if it exists.
     * Directories may only be deleted if they are empty.
     * This method is similar to Java 7 {@code java.nio.file.Path.deleteIfExists}.
     *
     * @param path the file or directory name
     */
    public static void delete(String path) throws IOException {
        FilePath.get(path).delete();
    }

    /**
     * Get the canonical file or directory name.
     * This method is similar to Java 7 {@code java.nio.file.Path.toRealPath}.
     *
     * @param fileName the file name
     * @return the normalized file name
     */
    public static String toRealPath(String fileName) throws IOException {
        return FilePath.get(fileName).toRealPath().toString();
    }

    /**
     * Get the parent directory of a file or directory.
     * This method returns null if there is no parent.
     * This method is similar to Java 7 {@code java.nio.file.Path.getParent}.
     *
     * @param fileName the file or directory name
     * @return the parent directory name
     */
    public static String getParent(String fileName) {
        FilePath p = FilePath.get(fileName).getParent();
        return p == null ? null : p.toString();
    }

    /**
     * Check if the file name includes a path.
     * This method is similar to Java 7 {@code java.nio.file.Path.isAbsolute}.
     *
     * @param fileName the file name
     * @return if the file name is absolute
     */
    public static boolean isAbsolute(String fileName) {
        return FilePath.get(fileName).isAbsolute();
    }

    /**
     * Rename a file if this is allowed.
     * This method is similar to Java 7 {@code java.nio.file.Path.moveTo}.
     *
     * @param oldName the old fully qualified file name
     * @param newName the new fully qualified file name
     */
    public static void moveTo(String oldName, String newName) throws IOException {
        FilePath.get(oldName).moveTo(FilePath.get(newName));
    }

    /**
     * Get the file or directory name (the last element of the path).
     * This method is similar to Java 7 {@code java.nio.file.Path.getName}.
     *
     * @param path the directory and file name
     * @return just the file name
     */
    public static String getName(String path) {
        return FilePath.get(path).getName();
    }

    /**
     * List the files and directories in the given directory.
     * This method is similar to Java 7 {@code java.nio.file.Path.newDirectoryStream}.
     *
     * @param path the directory
     * @return the list of fully qualified file names
     */
    public static List<String> newDirectoryStream(String path) throws IOException {
        List<FilePath> list = FilePath.get(path).newDirectoryStream();
        int len = list.size();
        List<String> result = new ArrayList<String>(len);
        for (int i = 0; i < len; i++) {
            result.add(list.get(i).toString());
        }
        return result;
    }

    /**
     * Get the last modified date of a file.
     * This method is similar to Java 7
     * {@code java.nio.file.attribute.Attributes.readBasicFileAttributes(file).lastModified().toMillis()}
     *
     * @param fileName the file name
     * @return the last modified date
     */
    public static long lastModified(String fileName) {
        return FilePath.get(fileName).lastModified();
    }

    /**
     * Get the size of a file in bytes
     * This method is similar to Java 7
     * {@code java.nio.file.attribute.Attributes.readBasicFileAttributes(file).size()}
     *
     * @param fileName the file name
     * @return the size in bytes
     */
    public static long size(String fileName) {
        return FilePath.get(fileName).size();
    }

    /**
     * Check if it is a file or a directory.
     * {@code java.nio.file.attribute.Attributes.readBasicFileAttributes(file).isDirectory()}
     *
     * @param fileName the file or directory name
     * @return true if it is a directory
     */
    public static boolean isDirectory(String fileName) {
        return FilePath.get(fileName).isDirectory();
    }

    /**
     * Open a random access file object.
     * This method is similar to Java 7 {@code java.nio.channels.FileChannel.open}.
     *
     * @param fileName the file name
     * @param mode the access mode. Supported are r, rw, rws, rwd
     * @return the file object
     */
    public static FileChannel open(String fileName, String mode) throws IOException {
        return FilePath.get(fileName).open(mode);
    }

    /**
     * Create an input stream to read from the file.
     * This method is similar to Java 7 {@code java.nio.file.Path.newInputStream}.
     *
     * @param fileName the file name
     * @return the input stream
     */
    public static InputStream newInputStream(String fileName) throws IOException {
        return FilePath.get(fileName).newInputStream();
    }

    /**
     * Create an output stream to write into the file.
     * This method is similar to Java 7 {@code java.nio.file.Path.newOutputStream}.
     *
     * @param fileName the file name
     * @param append if true, the file will grow, if false, the file will be
     *            truncated first
     * @return the output stream
     */
    public static OutputStream newOutputStream(String fileName, boolean append) throws IOException {
        return FilePath.get(fileName).newOutputStream(append);
    }

    /**
     * Check if the file is writable.
     * This method is similar to Java 7
     * {@code java.nio.file.Path.checkAccess(AccessMode.WRITE)}
     *
     * @param fileName the file name
     * @return if the file is writable
     */
    public static boolean canWrite(String fileName) {
        return FilePath.get(fileName).canWrite();
    }

    // special methods =======================================

    /**
     * Disable the ability to write. The file can still be deleted afterwards.
     *
     * @param fileName the file name
     * @return true if the call was successful
     */
    public static boolean setReadOnly(String fileName) {
        return FilePath.get(fileName).setReadOnly();
    }

    // utility methods =======================================

    /**
     * Delete a directory or file and all subdirectories and files.
     *
     * @param path the path
     * @param tryOnly whether errors should  be ignored
     */
    public static void deleteRecursive(String path, boolean tryOnly) throws IOException {
        if (exists(path)) {
            if (isDirectory(path)) {
                for (String s : newDirectoryStream(path)) {
                    deleteRecursive(s, tryOnly);
                }
            }
            if (tryOnly) {
                tryDelete(path);
            } else {
                delete(path);
            }
        }
    }

    /**
     * Create the directory and all required parent directories.
     *
     * @param dir the directory name
     */
    public static void createDirectories(String dir) throws IOException {
        if (dir != null) {
            if (exists(dir)) {
                if (!isDirectory(dir)) {
                    throw new IOException("Could not create directory, " +
                            "because a file with the same name already exists: " + dir);
                }
            } else {
                String parent = getParent(dir);
                createDirectories(parent);
                createDirectory(dir);
            }
        }
    }

    /**
     * Copy a file from one directory to another, or to another file.
     *
     * @param original the original file name
     * @param copy the file name of the copy
     */
    public static void copy(String original, String copy) throws IOException {
        InputStream in = newInputStream(original);
        try {
            OutputStream out = newOutputStream(copy, false);
            try {
                IOUtils.copy(in, out);
            } finally {
                out.close();
            }
        } finally {
            in.close();
        }
    }

    /**
     * Try to delete a file (ignore errors).
     *
     * @param fileName the file name
     * @return true if it worked
     */
    public static boolean tryDelete(String fileName) {
        try {
            FilePath.get(fileName).delete();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Create a new temporary file.
     *
     * @param prefix the prefix of the file name (including directory name if
     *            required)
     * @param suffix the suffix
     * @param deleteOnExit if the file should be deleted when the virtual
     *            machine exists
     * @param inTempDir if the file should be stored in the temporary directory
     * @return the name of the created file
     */
    public static String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        return FilePath.get(prefix).createTempFile(suffix, deleteOnExit, inTempDir).toString();
    }

    /**
     * Fully read from the file. This will read all remaining bytes,
     * or throw an EOFException if not successful.
     *
     * @param channel the file channel
     * @param dst the byte buffer
     */
    public static void readFully(FileChannel channel, ByteBuffer dst) throws IOException {
        do {
            int r = channel.read(dst);
            if (r < 0) {
                throw new EOFException();
            }
        } while (dst.remaining() > 0);
    }

    /**
     * Fully write to the file. This will write all remaining bytes.
     *
     * @param channel the file channel
     * @param src the byte buffer
     */
    public static void writeFully(FileChannel channel, ByteBuffer src) throws IOException {
        do {
            channel.write(src);
        } while (src.remaining() > 0);
    }

}
