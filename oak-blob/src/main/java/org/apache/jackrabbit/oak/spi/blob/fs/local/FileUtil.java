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
package org.apache.jackrabbit.oak.spi.blob.fs.local;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * Static utility methods for recursively copying and deleting files and
 * directories.
 */
public final class FileUtil {

    /**
     * private constructor
     */
    private FileUtil() {
    }

    /**
     * Recursively copies the given file or directory to the
     * given destination.
     *
     * @param src source file or directory
     * @param dest destination file or directory
     * @throws IOException if the file or directory cannot be copied
     */
    public static void copy(File src, File dest) throws IOException {
        if (!src.canRead()) {
            throw new IOException(src.getPath() + " can't be read from.");
        }
        if (src.isDirectory()) {
            // src is a folder
            if (dest.isFile()) {
                throw new IOException("can't copy a folder to a file");
            }
            if (!dest.exists()) {
                dest.mkdirs();
            }
            if (!dest.canWrite()) {
                throw new IOException("can't write to " + dest.getPath());
            }
            File[] children = src.listFiles();
            for (int i = 0; i < children.length; i++) {
                copy(children[i], new File(dest, children[i].getName()));
            }
        } else {
            // src is a file
            File destParent;
            if (dest.isDirectory()) {
                // dest is a folder
                destParent = dest;
                dest = new File(destParent, src.getName());
            } else {
                destParent = dest.getParentFile();
            }
            if (!destParent.canWrite()) {
                throw new IOException("can't write to " + destParent.getPath());
            }

            FileUtils.copyFile(src, dest);
        }
    }

    /**
     * Recursively deletes the given file or directory.
     *
     * @param f file or directory
     * @throws IOException if the file or directory cannot be deleted
     */
    public static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            // it's a folder, list children first
            File[] children = f.listFiles();
            for (int i = 0; i < children.length; i++) {
                delete(children[i]);
            }
        }
        if (!f.delete()) {
            throw new IOException("Unable to delete '" + f.getPath() + "'");
        }
    }
}
