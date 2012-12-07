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

package org.apache.jackrabbit.oak.namepath;

import java.util.Iterator;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.PathResolver;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * Static factories for {@link PathResolver} implementations.
 */
public class PathResolvers {
    private PathResolvers(){}

    /**
     * Create a path resolver, which interprets name, "." and ".." elements
     * in a path as the child of the given name of the current element, the current
     * element and the parent of the current element, respectively.
     * @param path  path to resolve
     * @return  a path resolver
     */
    public static PathResolver dotResolver(String path) {
        return new DotResolver(path);
    }

    /**
     * Create a path resolver, which interprets each element in a path as the child
     * of that name of the current element.
     * @param path  path to resolve
     * @return  a path resolver
     */
    public static PathResolver identity(String path) {
        return new IdResolver(path);
    }

    //------------------------------------------------------------< DotResolver >---

    /**
     * A path resolver, which interprets name, "." and ".." elements
     * in a path as the child of the given name of the current element, the current
     * element and the parent of the current element, respectively.
     */
    private static class DotResolver implements PathResolver {
        private final String path;

        public DotResolver(String path) {
            this.path = path;
        }

        @Override
        public Iterator<Function<TreeLocation, TreeLocation>> iterator() {
            return Iterators.transform(PathUtils.elements(path).iterator(),
                    new Function<String, Function<TreeLocation, TreeLocation>>() {
                @Nonnull
                @Override
                public Function<TreeLocation, TreeLocation> apply(final String element) {
                    if (PathUtils.denotesCurrent(element)) {
                        return new Function<TreeLocation, TreeLocation>() {
                            @Nonnull
                            @Override
                            public TreeLocation apply(TreeLocation location) {
                                return location;
                            }
                        };
                    }
                    else if (PathUtils.denotesParent(element)) {
                        return new Function<TreeLocation, TreeLocation>() {
                            @Nonnull
                            @Override
                            public TreeLocation apply(TreeLocation location) {
                                return location.getParent();
                            }
                        };
                    }
                    else {
                        return new Function<TreeLocation, TreeLocation>() {
                            @Nonnull
                            @Override
                            public TreeLocation apply(TreeLocation location) {
                                return location.getChild(element);
                            }
                        };
                    }
                }
            });
        }
    }

    //------------------------------------------------------------< IdResolver >---

    /**
     * A path resolver, which interprets each element in a path as the child
     * of that name of the current element.
     */
    private static class IdResolver implements PathResolver {
        private final String path;

        public IdResolver(String path) {
            this.path = path;
        }

        @Override
        public Iterator<Function<TreeLocation, TreeLocation>> iterator() {
            return Iterators.transform(PathUtils.elements(path).iterator(),
                    new Function<String, Function<TreeLocation, TreeLocation>>() {
                @Nonnull
                @Override
                public Function<TreeLocation, TreeLocation> apply(final String element) {
                    return new Function<TreeLocation, TreeLocation>() {
                        @Nonnull
                        @Override
                        public TreeLocation apply(TreeLocation location) {
                            return location.getChild(element);
                        }
                    };
                }
            });
        }
    }
}
