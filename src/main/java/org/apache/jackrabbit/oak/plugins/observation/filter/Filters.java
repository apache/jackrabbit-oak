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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This utility class provides common {@link Filter} instances
 */
public final class Filters {
    private Filters() {
    }

    /**
     * A filter that matches if and only if any of the filters passed to this
     * method matches.
     * @param filters  filters of which any must match
     * @return {@code true} if any of {@code filters} match.
     */
    public static Filter any(@Nonnull final Filter... filters) {
        return any(Lists.newArrayList(checkNotNull(filters)));
    }

    /**
     * A filter that matches if and only if all of the filters passed to this
     * method matches.
     * @param filters  filters of which all must match
     * @return {@code true} if all of {@code filters} match.
     */
    public static Filter all(@Nonnull final Filter... filters) {
        return all(Lists.newArrayList(checkNotNull(filters)));
    }

    private static Filter any(final List<Filter> filters) {
        return new Filter() {
            @Override
            public boolean includeAdd(PropertyState after) {
                for (Filter filter : filters) {
                    if (filter.includeAdd(after)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean includeChange(PropertyState before, PropertyState after) {
                for (Filter filter : filters) {
                    if (filter.includeChange(before, after)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean includeDelete(PropertyState before) {
                for (Filter filter : filters) {
                    if (filter.includeDelete(before)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean includeAdd(String name, NodeState after) {
                for (Filter filter : filters) {
                    if (filter.includeAdd(name, after)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean includeChange(String name, NodeState before, NodeState after) {
                for (Filter filter : filters) {
                    if (filter.includeChange(name, before, after)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean includeDelete(String name, NodeState before) {
                for (Filter filter : filters) {
                    if (filter.includeDelete(name, before)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
                for (Filter filter : filters) {
                    if (filter.includeMove(sourcePath, destPath, moved)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Filter create(String name, NodeState before, NodeState after) {
                List<Filter> childFilters = Lists.newArrayList();
                for (Filter filter : filters) {
                    Filter childFilter = filter.create(name, before, after);
                    if (childFilter != null) {
                        childFilters.add(childFilter);
                    }
                }
                return any(childFilters);
            }
        };
    }

    private static Filter all(final List<Filter> filters) {
        return new Filter() {
            @Override
            public boolean includeAdd(PropertyState after) {
                for (Filter filter : filters) {
                    if (!filter.includeAdd(after)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean includeChange(PropertyState before, PropertyState after) {
                for (Filter filter : filters) {
                    if (!filter.includeChange(before, after)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean includeDelete(PropertyState before) {
                for (Filter filter : filters) {
                    if (!filter.includeDelete(before)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean includeAdd(String name, NodeState after) {
                for (Filter filter : filters) {
                    if (!filter.includeAdd(name, after)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean includeChange(String name, NodeState before, NodeState after) {
                for (Filter filter : filters) {
                    if (!filter.includeChange(name, before, after)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean includeDelete(String name, NodeState before) {
                for (Filter filter : filters) {
                    if (!filter.includeDelete(name, before)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
                for (Filter filter : filters) {
                    if (!filter.includeMove(sourcePath, destPath, moved)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Filter create(String name, NodeState before, NodeState after) {
                List<Filter> childFilters = Lists.newArrayList();
                for (Filter filter : filters) {
                    Filter childFilter = filter.create(name, before, after);
                    if (childFilter == null) {
                        return null;
                    } else {
                        childFilters.add(childFilter);
                    }
                }
                return all(childFilters);
            }
        };
    }
}
