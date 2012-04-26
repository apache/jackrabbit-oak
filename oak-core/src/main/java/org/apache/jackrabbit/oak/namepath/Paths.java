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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.oak.namepath.JcrNameParser.Listener;

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
     * @param nth Integer indicating which ancestor path to retrieve.
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

    public static String toOakName(String name, final NameMapper mapper) {
        final StringBuilder element = new StringBuilder();
        
        Listener listener = new JcrNameParser.Listener() {
            @Override
            public void error(String message) {
                throw new RuntimeException(message);
            }

            @Override
            public void name(String name) {
                String p = mapper.getOakName(name);
                element.append(p);
            }
        };

        JcrNameParser.parse(name, listener);
        return element.toString();
    }
    
    public static String toOakPath(String jcrPath, final NameMapper mapper) {
        final List<String> elements = new ArrayList<String>();

        if ("/".equals(jcrPath)) {
            // avoid the need to special case the root path later on
            return "/";
        }

        JcrPathParser.Listener listener = new JcrPathParser.Listener() {

            // TODO: replace RuntimeException by something that oak-jcr can deal with (e.g. ValueFactory)

            @Override
            public void root() {
                if (!elements.isEmpty()) {
                    throw new RuntimeException("/ on non-empty path");
                }
                elements.add("");
            }

            @Override
            public void identifier(String identifier) {
                if (!elements.isEmpty()) {
                    throw new RuntimeException("[identifier] on non-empty path");
                }
                elements.add(identifier);  // todo resolve identifier
                // todo seal
            }

            @Override
            public void current() {
                // nothing to do here
            }

            @Override
            public void parent() {
                if (elements.isEmpty()) {
                    throw new RuntimeException(".. of empty path");
                }
                elements.remove(elements.size() - 1);
            }

            @Override
            public void index(int index) {
                if (index > 1) {
                    throw new RuntimeException("index > 1");
                }
            }

            @Override
            public void error(String message) {
                throw new RuntimeException(message);
            }

            @Override
            public void name(String name) {
                String p = mapper.getOakName(name);
                elements.add(p);
            }
        };

        JcrPathParser.parse(jcrPath, listener);
        
        StringBuilder oakPath = new StringBuilder();
        for (String element : elements) {
            if (element.isEmpty()) {
                // root
                oakPath.append('/');
            }
            else {
                oakPath.append(element);
                oakPath.append('/');
            }
        }
        
        // root path is special-cased early on so it does not need to
        // be considered here
        oakPath.deleteCharAt(oakPath.length() - 1);
        return oakPath.toString();
    }
    
    public static String toJcrPath(String oakPath, final NameMapper mapper) {
        final List<String> elements = new ArrayList<String>();
        
        if ("/".equals(oakPath)) {
            // avoid the need to special case the root path later on
            return "/";
        }

        JcrPathParser.Listener listener = new JcrPathParser.Listener() {
            @Override
            public void root() {
                if (!elements.isEmpty()) {
                    throw new RuntimeException("/ on non-empty path");
                }
                elements.add("");
            }

            @Override
            public void identifier(String identifier) {
                if (!elements.isEmpty()) {
                    throw new RuntimeException("[identifier] on non-empty path");
                }
                elements.add(identifier);  // todo resolve identifier
                // todo seal
            }

            @Override
            public void current() {
                // nothing to do here
            }

            @Override
            public void parent() {
                if (elements.isEmpty()) {
                    throw new RuntimeException(".. of empty path");
                }
                elements.remove(elements.size() - 1);
            }

            @Override
            public void index(int index) {
                if (index > 1) {
                    throw new RuntimeException("index > 1");
                }
            }

            @Override
            public void error(String message) {
                throw new RuntimeException(message);
            }

            @Override
            public void name(String name) {
                String p = mapper.getJcrName(name);
                elements.add(p);
            }
        };

        JcrPathParser.parse(oakPath, listener);
        
        StringBuilder jcrPath = new StringBuilder();
        for (String element : elements) {
            if (element.isEmpty()) {
                // root
                jcrPath.append('/');
            }
            else {
                jcrPath.append(element);
                jcrPath.append('/');
            }
        }
        
        jcrPath.deleteCharAt(jcrPath.length() - 1);
        return jcrPath.toString();
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
