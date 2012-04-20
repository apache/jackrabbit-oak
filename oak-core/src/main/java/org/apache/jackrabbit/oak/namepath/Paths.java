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
package org.apache.jackrabbit.oak.namepath;

import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.oak.util.Function1;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * All path in the Oak API have the following form
 * <p>
 * <pre>
 * PATH    := ("/" ELEMENT)* | ELEMENT ("/" ELEMENT)*
 * ELEMENT := [PREFIX ":"] NAME
 * PREFIX  := non empty string not containing ":" and "/"
 * NAME    := non empty string not containing ":" and "/" TODO: check whether this is correct
 * </pre>
 */
public final class Paths {

    private Paths() {}

    /**
     * Get the parent of a path. The parent of the root path ("/") is the root
     * path.
     *
     * @param path the path
     * @return the parent path
     */
    public static String getParentPath(String path) {
        return PathUtils.getParentPath(path);
    }

    /**
     * Get the last element of the (absolute or relative) path. The name of the
     * root node ("/") and the name of the empty path ("") is the empty path.
     *
     * @param path the complete path
     * @return the last element
     */
    public static String getName(String path) {
        return PathUtils.getName(path);
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
        return PathUtils.getAncestorPath(path, nth);
    }

    /**
     * Concatenate path elements.
     *
     * @param parentPath the parent path
     * @param subPath the subPath path to add
     * @return the concatenated path
     */
    public static String concat(String parentPath, String subPath) {
        return PathUtils.concat(parentPath, subPath);
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
        return PathUtils.relativize(parentPath, path);
    }

    /**
     * Calculate the number of elements in the path. The root path has zero
     * elements.
     *
     * @param path the path
     * @return the number of elements
     */
    public static int getDepth(String path) {
        return PathUtils.getDepth(path);
    }

    /**
     * Get the prefix of an element. Undefined if {@code element} is
     * not an {@code ELEMENT}.
     * @param element
     * @return  the {@code PREFIX} of {@code element} or {@code null} if none
     */
    public static String getPrefixFromElement(String element) {
        int pos = element.indexOf(':');
        if (pos == -1) {
            return null;
        }
        else {
            return element.substring(0, pos);
        }
    }

    /**
     * Get the name of an element. Undefined if {@code element} is
     * not an {@code ELEMENT}.
     * @param element
     * @return  the {@code NAME} of {@code element}
     */
    public static String getNameFromElement(String element) {
        int pos = element.indexOf(':');
        if (pos == -1) {
            return element;
        }
        else {
            return element.substring(pos + 1);
        }
    }

    /**
     * Determine whether {@code string} is a valid {@code ELEMENT}.
     * @param string
     * @return  {@code true} iff {@code string} is a valid {@code ELEMENT}.
     */
    public static boolean isValidElement(String string) {
        if (string.isEmpty()) {
            return false;
        }

        int colons = 0;
        int pos = -1;
        for (int k = 0; k < string.length(); k++) {
            if (string.charAt(k) == ':') {
                colons += 1;
                pos = k;
            }
            else if (string.charAt(k) == '/') {
                return false;
            }
        }

        return colons <= 1 && (pos != 0 && pos != string.length() - 1);
    }

    /**
     * Determine whether {@code string} is a valid {@code PATH}.
     * @param string
     * @return  {@code true} iff {@code string} is a valid {@code PATH}.
     */
    public static boolean isValidPath(String string) {
        if (string.isEmpty()) {
            return false;
        }
        if (string.length() > 1 && string.endsWith("/")) {
            return false;
        }

        for (String part : split(string, '/')) {
            if (!isValidElement(part)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Resolve {@code element} against {@code prefixResolver}: replace the
     * prefix of this element with the prefix returned by the prefix resolver.
     * Undefined if {@code element} is not a valid {@code ELEMENT}.
     * @param element  {@code ELEMENT}
     * @param prefixResolver  prefix resolver
     * @return  resolved element
     * @throws IllegalArgumentException  if {@code prefixResolver} returns {@code null}
     * for the prefix of {@code element}.
     */
    public static String resolveElement(String element, Function1<String, String> prefixResolver) {
        String prefix = getPrefixFromElement(element);
        if (prefix == null) {
            return element;
        }

        String newPrefix = prefixResolver.apply(prefix);
        if (newPrefix == null) {
            throw new IllegalArgumentException("Can't resolve prefix " + prefix);
        }
        return newPrefix + ':' + getNameFromElement(element);
    }

    /**
     * Resolve {@code path} against {@code prefixResolver} by resolving each
     * element of the path against that prefix resolver.
     * @param path  {@code PATH}
     * @param prefixResolver  prefix resolver
     * @return  resolved path
     * @throws IllegalArgumentException if {@code prefixResolver} returns {@code null}
     * for a prefix in any of the elements of {@code path}.
     */
    public static String resolvePath(String path, Function1<String, String> prefixResolver) {
        StringBuilder resolved = new StringBuilder();

        String sep = path.charAt(0) == '/' ? "/" : "";

        for (String element : split(path, '/')) {
            resolved.append(sep).append(resolveElement(element, prefixResolver));
            sep = "/";
        }
        
        return resolved.toString();
    }
    
    //------------------------------------------------------------< private >--- 

    private static Iterable<String> split(final String string, final char separator) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    int pos = !string.isEmpty() && string.charAt(0) == separator ? 1 : 0;
                    String next;

                    @Override
                    public boolean hasNext() {
                        if (next == null) {
                            if (pos >= string.length()) {
                                return false;
                            }
                            else {
                                int i = string.indexOf(separator, pos);
                                if (i < 0) {
                                    next = string.substring(pos);
                                    pos = string.length();
                                }
                                else {
                                    next = string.substring(pos, i);
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
            }
        };
    }

}
