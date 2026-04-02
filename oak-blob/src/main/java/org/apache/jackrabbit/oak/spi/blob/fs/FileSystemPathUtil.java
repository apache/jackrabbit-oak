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
// copied from Apache Jackrabbit jackrabbit-data module; original class org.apache.jackrabbit.core.fs.FileSystemPathUtil
package org.apache.jackrabbit.oak.spi.blob.fs;

import java.io.ByteArrayOutputStream;
import java.util.BitSet;


/**
 * Utility class for handling paths in a file system.
 */
public final class FileSystemPathUtil {

    /**
     * The list of characters that are not encoded by the <code>escapeName(String)</code>
     * and <code>unescape(String)</code> methods. They contains the characters
     * which can safely be used in file names:
     */
    public static final BitSet SAFE_NAMECHARS;

    /**
     * The list of characters that are not encoded by the <code>escapePath(String)</code>
     * and <code>unescape(String)</code> methods. They contains the characters
     * which can safely be used in file paths:
     */
    public static final BitSet SAFE_PATHCHARS;

    static {
        // build list of valid name characters
        SAFE_NAMECHARS = new BitSet(256);
        int i;
        for (i = 'a'; i <= 'z'; i++) {
            SAFE_NAMECHARS.set(i);
        }
        for (i = 'A'; i <= 'Z'; i++) {
            SAFE_NAMECHARS.set(i);
        }
        for (i = '0'; i <= '9'; i++) {
            SAFE_NAMECHARS.set(i);
        }
        SAFE_NAMECHARS.set('-');
        SAFE_NAMECHARS.set('_');
        SAFE_NAMECHARS.set('.');

        // build list of valid path characters (includes name characters)
        SAFE_PATHCHARS = (BitSet) SAFE_NAMECHARS.clone();
        SAFE_PATHCHARS.set(FileSystem.SEPARATOR_CHAR);
    }

    /**
     * private constructor
     */
    private FileSystemPathUtil() {
    }

    /**
     * Tests whether the specified path represents the root path, i.e. "/".
     *
     * @param path path to test
     * @return true if the specified path represents the root path; false otherwise.
     */
    public static boolean denotesRoot(String path) {
        return path.equals(FileSystem.SEPARATOR);
    }

    /**
     * Returns the parent directory of the specified <code>path</code>.
     *
     * @param path a file system path denoting a directory or a file.
     * @return the parent directory.
     */
    public static String getParentDir(String path) {
        int pos = path.lastIndexOf(FileSystem.SEPARATOR_CHAR);
        if (pos > 0) {
            return path.substring(0, pos);
        }
        return FileSystem.SEPARATOR;
    }

    /**
     * Returns the name of the specified <code>path</code>.
     *
     * @param path a file system path denoting a directory or a file.
     * @return the name.
     */
    public static String getName(String path) {
        int pos = path.lastIndexOf(FileSystem.SEPARATOR_CHAR);
        if (pos != -1) {
            return path.substring(pos + 1);
        }
        return path;
    }

}
