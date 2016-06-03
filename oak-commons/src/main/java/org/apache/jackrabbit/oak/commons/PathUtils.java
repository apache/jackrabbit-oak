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

import static com.google.common.collect.Sets.newHashSet;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Utility methods to parse a path.
 * <p>
 * Each method validates the input, except if the system property
 * {packageName}.SKIP_VALIDATION is set, in which case only minimal validation
 * takes place within this function, so when the parameter is an illegal path,
 * the the result of this method is undefined.
 */
public final class PathUtils {

    public static final String ROOT_PATH = "/";
    public static final String ROOT_NAME = "";

    private static final Pattern SNS_PATTERN =
            Pattern.compile("(.+)\\[[1-9][0-9]*\\]$");

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
        assert isValid(path) : "Invalid path ["+path+"]";

        return denotesRootPath(path);
    }

    private static boolean denotesRootPath(String path) {
        return ROOT_PATH.equals(path);
    }

    /**
     * @param element The path segment to check for being the current element
     * @return {@code true} if the specified element equals "."; {@code false} otherwise.
     */
    public static boolean denotesCurrent(String element) {
        return ".".equals(element);
    }

    /**
     * @param element The path segment to check for being the parent element
     * @return {@code true} if the specified element equals ".."; {@code false} otherwise.
     */
    public static boolean denotesParent(String element) {
        return "..".equals(element);
    }

    /**
     * Whether the path is absolute (starts with a slash) or not.
     *
     * @param path the path
     * @return true if it starts with a slash
     */
    public static boolean isAbsolute(String path) {
        assert isValid(path) : "Invalid path ["+path+"]";

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
     * <p>
     * If {@code nth <= 0}, the path argument is returned as is.
     *
     * @param path the path
     * @param nth  indicates the ancestor level for which the path should be
     *             calculated.
     * @return the ancestor path
     */
    @Nonnull
    public static String getAncestorPath(String path, int nth) {
        assert isValid(path) : "Invalid path ["+path+"]";

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
                return ROOT_PATH;
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
        assert isValid(path) : "Invalid path ["+path+"]";

        if (path.isEmpty() || denotesRootPath(path)) {
            return ROOT_NAME;
        }
        int end = path.length() - 1;
        int pos = path.lastIndexOf('/', end);
        if (pos != -1) {
            return path.substring(pos + 1, end + 1);
        }
        return path;
    }

    /**
     * Returns the given name without the possible SNS index suffix. If the
     * name does not contain an SNS index, then it is returned as-is.
     *
     * @param name name with a possible SNS index suffix
     * @return name without the SNS index suffix
     */
    @Nonnull
    public static String dropIndexFromName(@Nonnull String name) {
        Matcher matcher = SNS_PATTERN.matcher(name);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return name;
    }

    /**
     * Calculate the number of elements in the path. The root path has zero
     * elements.
     *
     * @param path the path
     * @return the number of elements
     */
    public static int getDepth(String path) {
        assert isValid(path) : "Invalid path ["+path+"]";

        if (path.isEmpty()) {
            return 0;
        }
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
        assert isValid(path) : "Invalid path ["+path+"]";

        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    int pos = isAbsolute(path) ? 1 : 0;
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
            }
        };
    }

    /**
     * Concatenate path elements.
     *
     * @param parentPath    the parent path
     * @param relativePaths the relative path elements to add
     * @return the concatenated path
     */
    @Nonnull
    public static String concat(String parentPath, String... relativePaths) {
        assert isValid(parentPath) : "Invalid parent path ["+parentPath+"]";
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
     * @param subPath    the subPath path to add
     * @return the concatenated path
     */
    @Nonnull
    public static String concat(String parentPath, String subPath) {
        assert isValid(parentPath) : "Invalid parent path ["+parentPath+"]";
        assert isValid(subPath) : "Invalid sub path ["+subPath+"]";
        // special cases
        if (parentPath.isEmpty()) {
            return subPath;
        } else if (subPath.isEmpty()) {
            return parentPath;
        } else if (isAbsolutePath(subPath)) {
            throw new IllegalArgumentException("Cannot append absolute path " + subPath);
        }
        StringBuilder buff = new StringBuilder(parentPath.length() + subPath.length() + 1);
        buff.append(parentPath);
        if (!denotesRootPath(parentPath)) {
            buff.append('/');
        }
        buff.append(subPath);
        return buff.toString();
    }

    /**
     * Relative path concatenation.
     *
     * @param relativePaths relative paths
     * @return the concatenated path or {@code null} if the resulting path is empty.
     */
    @CheckForNull
    public static String concatRelativePaths(String... relativePaths) {
        StringBuilder result = new StringBuilder();
        for (String path : relativePaths) {
            if (path != null && !path.isEmpty()) {
                int i0 = 0;
                int i1 = path.length();
                while (i0 < i1 && path.charAt(i0) == '/') {
                    i0++;
                }
                while (i1 > i0 && path.charAt(i1-1) == '/') {
                    i1--;
                }
                if (i1 > i0) {
                    if (result.length() > 0) {
                        result.append('/');
                    }
                    result.append(path.substring(i0, i1));
                }
            }
        }
        return result.length() == 0 ? null : result.toString();
    }

    /**
     * Check if a path is a (direct or indirect) ancestor of another path.
     *
     * @param ancestor the ancestor path
     * @param path     the potential offspring path
     * @return true if the path is an offspring of the ancestor
     */
    public static boolean isAncestor(String ancestor, String path) {
        assert isValid(ancestor) : "Invalid parent path ["+ancestor+"]";
        assert isValid(path) : "Invalid path ["+path+"]";
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
     * @param path       path to relativize
     * @return relativized path
     */
    @Nonnull
    public static String relativize(String parentPath, String path) {
        assert isValid(parentPath) : "Invalid parent path ["+parentPath+"]";
        assert isValid(path) : "Invalid path ["+path+"]";

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
     * @param path  the path
     * @param index the starting index
     * @return the index of the next slash (possibly the starting index), or -1
     *         if not found
     */
    public static int getNextSlash(String path, int index) {
        assert isValid(path) : "Invalid path ["+path+"]";

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

    /**
     * Unify path inclusions and exclusions.
     * <ul>
     * <li>A path in {@code includePaths} is only retained if {@code includePaths} contains
     * none of its ancestors and {@code excludePaths} contains neither of its ancestors nor
     * that path itself.</li>
     * <li>A path in {@code excludePaths} is only retained if {@code includePaths} contains
     * an ancestor of that path.</li>
     * </ul>
     *
     * When a set of paths is <em>filtered wrt.</em> {@code includePaths} and {@code excludePaths}
     * by first excluding all paths that have an ancestor or are contained in {@code excludePaths}
     * and then including all paths that have an ancestor or are contained in {@code includePaths}
     * then the result is the same regardless whether the {@code includePaths} and
     * {@code excludePaths} sets have been run through this method or not.
     *
     * @param includePaths set of paths to be included
     * @param excludedPaths set of paths to be excluded
     */
    public static void unifyInExcludes(Set<String> includePaths, Set<String> excludedPaths) {
        Set<String> retain = newHashSet();
        Set<String> includesRemoved = newHashSet();
        for (String include : includePaths) {
            for (String exclude : excludedPaths) {
                if (exclude.equals(include) || isAncestor(exclude, include)) {
                    includesRemoved.add(include);
                } else if (isAncestor(include, exclude)) {
                    retain.add(exclude);
                }
            }

            //Remove redundant includes /a, /a/b -> /a
            for (String include2 : includePaths) {
                if (isAncestor(include, include2)) {
                    includesRemoved.add(include2);
                }
            }
        }
        includePaths.removeAll(includesRemoved);
        excludedPaths.retainAll(retain);
    }


}
