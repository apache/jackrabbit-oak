/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

/**
 * Utility methods to parse a path.
 * <p/>
 * Each method validates the input, except if the system property
 * {packageName}.SKIP_VALIDATION is set, in which case only minimal validation
 * takes place within this function, so when the parameter is an illegal path,
 * the the result of this method is undefined.
 */
public class PathUtils {

    private PathUtils() {
        // utility class
    }

    /**
     * Whether the path is the root path ("/").
     *
     * @param path the path
     * @return whether this is the root
     */
    public static boolean denotesRoot(String path) {
        assert isValid(path);

        return denotesRootPath(path);
    }

    private static boolean denotesRootPath(String path) {
        return "/".equals(path);
    }

    /**
     * Whether the path is absolute (starts with a slash) or not.
     *
     * @param path the path
     * @return true if it starts with a slash
     */
    public static boolean isAbsolute(String path) {
        assert isValid(path);

        return isAbsolutePath(path);
    }

    private static boolean isAbsolutePath(String path) {
        return !path.isEmpty() && path.charAt(0) == '/';
    }

    /**
     * Get the parent of a path. The parent of the root path ("/") is the root
     * path.
     *
     * @param path the path
     * @return the parent path
     */
    @Nonnull
    public static String getParentPath(String path) {
        return getAncestorPath(path, 1);
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
    @Nonnull
    public static String getAncestorPath(String path, int nth) {
        assert isValid(path);

        if (path.isEmpty() || denotesRootPath(path)
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
     * Get the last element of the (absolute or relative) path. The name of the
     * root node ("/") and the name of the empty path ("") is the empty path.
     *
     * @param path the complete path
     * @return the last element
     */
    @Nonnull
    public static String getName(String path) {
        assert isValid(path);

        if (path.isEmpty() || denotesRootPath(path)) {
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
     * Calculate the number of elements in the path. The root path has zero
     * elements.
     *
     * @param path the path
     * @return the number of elements
     */
    public static int getDepth(String path) {
        assert isValid(path);

        int count = 1, i = 0;
        if (isAbsolutePath(path)) {
            if (denotesRootPath(path)) {
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
     * Returns an {@code Iterable} for the path elements. The root path ("/") and the
     * empty path ("") have zero elements.
     *
     * @param path the path
     * @return an Iterable for the path elements
     */
    @Nonnull
    public static Iterable<String> elements(final String path) {
        assert isValid(path);

        final Iterator<String> it = new Iterator<String>() {
            int pos = PathUtils.isAbsolute(path) ? 1 : 0;
            String next;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    if (pos >= path.length()) {
                        return false;
                    }
                    int i = path.indexOf('/', pos);
                    if (i < 0) {
                        next = path.substring(pos);
                        pos = path.length();
                    } else {
                        next = path.substring(pos, i);
                        pos = i + 1;
                    }
                }
                return true;
            }

            @Override
            public String next() {
                if (hasNext()) {
                    String next = this.next;
                    this.next = null;
                    return next;
                }
                throw new NoSuchElementException();
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

    /**
     * Concatenate path elements.
     *
     * @param parentPath the parent path
     * @param relativePaths the relative path elements to add
     * @return the concatenated path
     */
    @Nonnull
    public static String concat(String parentPath, String... relativePaths) {
        assert isValid(parentPath);
        int parentLen = parentPath.length();
        int size = relativePaths.length;
        StringBuilder buff = new StringBuilder(parentLen + size * 5);
        buff.append(parentPath);
        boolean needSlash = parentLen > 0 && !denotesRootPath(parentPath);
        for (String s : relativePaths) {
            assert isValid(s);
            if (isAbsolutePath(s)) {
                throw new IllegalArgumentException("Cannot append absolute path " + s);
            }
            if (!s.isEmpty()) {
                if (needSlash) {
                    buff.append('/');
                }
                buff.append(s);
                needSlash = true;
            }
        }
        return buff.toString();
    }

    /**
     * Concatenate path elements.
     *
     * @param parentPath the parent path
     * @param subPath the subPath path to add
     * @return the concatenated path
     */
    @Nonnull
    public static String concat(String parentPath, String subPath) {
        assert isValid(parentPath);
        assert isValid(subPath);
        // special cases
        if (parentPath.isEmpty()) {
            return subPath;
        } else if (subPath.isEmpty()) {
            return parentPath;
        } else if (isAbsolutePath(subPath)) {
            throw new IllegalArgumentException("Cannot append absolute path " + subPath);
        }
        StringBuilder buff = new StringBuilder(parentPath);
        if (!denotesRootPath(parentPath)) {
            buff.append('/');
        }
        buff.append(subPath);
        return buff.toString();
    }

    /**
     * Check if a path is a (direct or indirect) ancestor of another path.
     *
     * @param ancestor the ancestor path
     * @param path the potential offspring path
     * @return true if the path is an offspring of the ancestor
     */
    public static boolean isAncestor(String ancestor, String path) {
        assert isValid(ancestor);
        assert isValid(path);
        if (ancestor.isEmpty() || path.isEmpty()) {
            return false;
        }
        if (denotesRoot(ancestor)) {
            if (denotesRoot(path)) {
                return false;
            }
        } else {
            ancestor += "/";
        }
        return path.startsWith(ancestor);
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
    @Nonnull
    public static String relativize(String parentPath, String path) {
        assert isValid(parentPath);
        assert isValid(path);

        if (parentPath.equals(path)) {
            return "";
        }

        String prefix = denotesRootPath(parentPath)
                ? parentPath
                : parentPath + '/';

        if (path.startsWith(prefix)) {
            return path.substring(prefix.length());
        }
        throw new IllegalArgumentException("Cannot relativize " + path + " wrt. " + parentPath);
    }

    /**
     * Get the index of the next slash.
     *
     * @param path the path
     * @param index the starting index
     * @return the index of the next slash (possibly the starting index), or -1
     *         if not found
     */
    public static int getNextSlash(String path, int index) {
        assert isValid(path);

        return path.indexOf('/', index);
    }

    /**
     * Check if the path is valid, and throw an IllegalArgumentException if not.
     * A valid path is absolute (starts with a '/') or relative (doesn't start
     * with '/'), and contains none or more elements. A path may not end with
     * '/', except for the root path. Elements itself must be at least one
     * character long.
     *
     * @param path the path
     */
    public static void validate(String path) {
        if (path.isEmpty() || denotesRootPath(path)) {
            return;
        } else if (path.charAt(path.length() - 1) == '/') {
            throw new IllegalArgumentException("Path may not end with '/': " + path);
        }
        char last = 0;
        for (int index = 0, len = path.length(); index < len; index++) {
            char c = path.charAt(index);
            if (c == '/') {
                if (last == '/') {
                    throw new IllegalArgumentException("Path may not contains '//': " + path);
                }
            }
            last = c;
        }
    }

    /**
     * Check if the path is valid. A valid path is absolute (starts with a '/')
     * or relative (doesn't start with '/'), and contains none or more elements.
     * A path may not end with '/', except for the root path. Elements itself must
     * be at least one character long.
     *
     * @param path the path
     * @return {@code true} iff the path is valid.
     */
    public static boolean isValid(String path) {
        if (path.isEmpty() || denotesRootPath(path)) {
            return true;
        } else if (path.charAt(path.length() - 1) == '/') {
            return false;
        }
        char last = 0;
        for (int index = 0, len = path.length(); index < len; index++) {
            char c = path.charAt(index);
            if (c == '/') {
                if (last == '/') {
                    return false;
                }
            }
            last = c;
        }
        return true;
    }

}
