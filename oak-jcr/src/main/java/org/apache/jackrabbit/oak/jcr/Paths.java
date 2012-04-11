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
package org.apache.jackrabbit.oak.jcr;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public class Paths {

    private Paths() {
    }

    /**
     * Whether the path is absolute (starts with a slash) or not.
     *
     * @param path the path
     * @return true if it starts with a slash
     */
    public static boolean isAbsolute(String path) {
        return !path.isEmpty() && path.charAt(0) == '/';
    }

    /**
     * Whether the path is the root path ("/").
     *
     * @param path the path
     * @return whether this is the root
     */
    public static boolean denotesRoot(String path) {
        return "/".equals(path);
    }

    /**
     * Get the parent of a path. The parent of the root path ("/") is the root
     * path.
     *
     * @param path the path
     * @return the parent path
     */
    public static String getParentPath(String path) {
        return getAncestorPath(path, 1);
    }

    /**
     * Get the last element of the (absolute or relative) path. The name of the
     * root node ("/") and the name of the empty path ("") is the empty path.
     *
     * @param path the complete path
     * @return the last element
     */
    public static String getName(String path) {
        if (path.isEmpty() || denotesRoot(path)) {
            return "";
        }
        int end = path.length() - 1;
        int pos = path.lastIndexOf('/', end);
        if (pos != -1) {
            return path.substring(pos + 1, end + 1);
        }
        return path;
    }

    /**
     * Get the nth ancestor of a path. The 1st ancestor is the parent path,
     * 2nd ancestor the grandparent path, and so on...
     * <p/>
     * If nth <= 0, the path argument is returned as is.
     *
     * @param path the path
     * @return the ancestor path
     */
    public static String getAncestorPath(String path, int nth) {
        if (path.isEmpty() || denotesRoot(path)
                || nth <= 0) {
            return path;
        }

        int end = path.length() - 1;
        int pos = -1;
        while (nth-- > 0) {
            pos = path.lastIndexOf('/', end);
            if (pos > 0) {
                end = pos - 1;
            } else if (pos == 0) {
                return "/";
            } else {
                return "";
            }
        }

        return path.substring(0, pos);
    }

    /**
     * Concatenate path elements.
     *
     * @param parentPath the parent path
     * @param subPath the subPath path to add
     * @return the concatenated path
     */
    public static String concat(String parentPath, String subPath) {
        // special cases
        if (parentPath.isEmpty()) {
            return subPath;
        } else if (subPath.isEmpty()) {
            return parentPath;
        } else if (isAbsolute(subPath)) {
            throw new IllegalArgumentException("Cannot append absolute path " + subPath);
        }
        StringBuilder buff = new StringBuilder(parentPath);
        if (!denotesRoot(parentPath)) {
            buff.append('/');
        }
        buff.append(subPath);
        return buff.toString();
    }

    /**
     * Relativize a path wrt. a parent path such that
     * {@code relativize(parentPath, concat(parentPath, path)) == paths}
     * holds.
     *
     * @param parentPath parent pth
     * @param path path to relativize
     * @return relativized path
     */
    public static String relativize(String parentPath, String path) {
        if (parentPath.equals(path)) {
            return "";
        }

        String prefix = denotesRoot(parentPath)
                ? parentPath
                : parentPath + '/';

        if (path.startsWith(prefix)) {
            return path.substring(prefix.length());
        }
        throw new IllegalArgumentException("Cannot relativize " + path + " wrt. " + parentPath);
    }

    /**
     * Calculate the number of elements in the path. The root path has zero
     * elements.
     *
     * @param path the path
     * @return the number of elements
     */
    public static int getDepth(String path) {
        int count = 1, i = 0;
        if (isAbsolute(path)) {
            if (denotesRoot(path)) {
                return 0;
            }
            i++;
        }
        while (true) {
            i = path.indexOf('/', i) + 1;
            if (i == 0) {
                return count;
            }
            count++;
        }
    }

    /**
     * Split a path into elements. The root path ("/") and the empty path ("")
     * is zero elements.
     *
     * @param path the path
     * @return an Iterable for the path elements
     */
    public static Iterable<String> elements(final String path) {
        final Iterator<String> it = new Iterator<String>() {
            int pos = isAbsolute(path) ? 1 : 0;
            String next;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    if (pos >= path.length()) {
                        return false;
                    }
                    else {
                        int i = path.indexOf('/', pos);
                        if (i < 0) {
                            next = path.substring(pos);
                            pos = path.length();
                        }
                        else {
                            next = path.substring(pos, i);
                            pos = i + 1;
                        }
                        return true;
                    }
                }
                else {
                    return true;
                }
            }

            @Override
            public String next() {
                if (hasNext()) {
                    String next = this.next;
                    this.next = null;
                    return next;
                }
                else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        };

        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return it;
            }
        };
    }


}
