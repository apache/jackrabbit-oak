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
import static org.apache.jackrabbit.oak.plugins.observation.filter.ConstantFilter.EXCLUDE_ALL;
import static org.apache.jackrabbit.oak.plugins.observation.filter.ConstantFilter.INCLUDE_ALL;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This utility class provides common {@link EventFilter} instances
 */
public final class Filters {

    private Filters() {
    }

    /**
     * A filter that matches if and only if any the filter passed to this
     * method does not match.
     * @param filter  filter which must not match
     * @return {@code true} if {@code filter} does not match.
     */
    @Nonnull
    public static EventFilter not(@Nonnull final EventFilter filter) {
        return new EventFilter() {
            @Override
            public boolean includeAdd(PropertyState after) {
                return !filter.includeAdd(after);
            }

            @Override
            public boolean includeChange(PropertyState before, PropertyState after) {
                return !filter.includeChange(before, after);
            }

            @Override
            public boolean includeDelete(PropertyState before) {
                return !filter.includeDelete(before);
            }

            @Override
            public boolean includeAdd(String name, NodeState after) {
                return !filter.includeAdd(name, after);
            }

            @Override
            public boolean includeDelete(String name, NodeState before) {
                return !filter.includeDelete(name, before);
            }

            @Override
            public boolean includeMove(String sourcePath, String name, NodeState moved) {
                return !filter.includeMove(sourcePath, name, moved);
            }

            @Override
            public boolean includeReorder(String destName, String name, NodeState reordered) {
                return !filter.includeReorder(destName, name, reordered);
            }

            @Override
            public EventFilter create(String name, NodeState before, NodeState after) {
                EventFilter childFilter = filter.create(name, before, after);
                return childFilter == null
                    ? INCLUDE_ALL
                    : not(childFilter);
            }
        };
    }

    /**
     * A filter that matches if and only if any of the filters passed to this
     * method matches.
     * @param filters  filters of which any must match
     * @return {@code true} if any of {@code filters} match.
     */
    @Nonnull
    public static EventFilter any(@Nonnull final EventFilter... filters) {
        return any(Lists.newArrayList(checkNotNull(filters)));
    }

    /**
     * A filter that matches if and only if all of the filters passed to this
     * method matches.
     * @param filters  filters of which all must match
     * @return {@code true} if all of {@code filters} match.
     */
    @Nonnull
    public static EventFilter all(@Nonnull final EventFilter... filters) {
        return all(Lists.newArrayList(checkNotNull(filters)));
    }

    /**
     * @return  Filter that includes everything
     */
    @Nonnull
    public static EventFilter includeAll() {
        return INCLUDE_ALL;
    }

    /**
     * @return  Filter that excludes everything
     */
    @Nonnull
    public static EventFilter excludeAll() {
        return EXCLUDE_ALL;
    }

    /**
     * A filter that matches if and only if any of the filters passed to this
     * method matches.
     * @param filters  filters of which any must match
     * @return {@code true} if any of {@code filters} match.
     */
    @Nonnull
    public static EventFilter any(@Nonnull final List<EventFilter> filters) {
        if (checkNotNull(filters).isEmpty()) {
            return EXCLUDE_ALL;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            return new EventFilter() {
                @Override
                public boolean includeAdd(PropertyState after) {
                    for (EventFilter filter : filters) {
                        if (filter.includeAdd(after)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public boolean includeChange(PropertyState before, PropertyState after) {
                    for (EventFilter filter : filters) {
                        if (filter.includeChange(before, after)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public boolean includeDelete(PropertyState before) {
                    for (EventFilter filter : filters) {
                        if (filter.includeDelete(before)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public boolean includeAdd(String name, NodeState after) {
                    for (EventFilter filter : filters) {
                        if (filter.includeAdd(name, after)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public boolean includeDelete(String name, NodeState before) {
                    for (EventFilter filter : filters) {
                        if (filter.includeDelete(name, before)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public boolean includeMove(String sourcePath, String name, NodeState moved) {
                    for (EventFilter filter : filters) {
                        if (filter.includeMove(sourcePath, name, moved)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public boolean includeReorder(String destName, String name, NodeState reordered) {
                    for (EventFilter filter : filters) {
                        if (filter.includeReorder(destName, name, reordered)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public EventFilter create(String name, NodeState before, NodeState after) {
                    List<EventFilter> childFilters = Lists.newArrayList();
                    for (EventFilter filter : filters) {
                        EventFilter childFilter = filter.create(name, before, after);
                        if (childFilter != null) {
                            childFilters.add(childFilter);
                        }
                    }
                    return any(childFilters);
                }
            };
        }
    }

    /**
     * A filter that matches if and only if all of the filters passed to this
     * method matches.
     * @param filters  filters of which all must match
     * @return {@code true} if all of {@code filters} match.
     */
    @Nonnull
    public static EventFilter all(@Nonnull final List<EventFilter> filters) {
        if (checkNotNull(filters).isEmpty()) {
            return INCLUDE_ALL;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            return new EventFilter() {
                @Override
                public boolean includeAdd(PropertyState after) {
                    for (EventFilter filter : filters) {
                        if (!filter.includeAdd(after)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public boolean includeChange(PropertyState before, PropertyState after) {
                    for (EventFilter filter : filters) {
                        if (!filter.includeChange(before, after)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public boolean includeDelete(PropertyState before) {
                    for (EventFilter filter : filters) {
                        if (!filter.includeDelete(before)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public boolean includeAdd(String name, NodeState after) {
                    for (EventFilter filter : filters) {
                        if (!filter.includeAdd(name, after)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public boolean includeDelete(String name, NodeState before) {
                    for (EventFilter filter : filters) {
                        if (!filter.includeDelete(name, before)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public boolean includeMove(String sourcePath, String name, NodeState moved) {
                    for (EventFilter filter : filters) {
                        if (!filter.includeMove(sourcePath, name, moved)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public boolean includeReorder(String destName, String name, NodeState reordered) {
                    for (EventFilter filter : filters) {
                        if (!filter.includeReorder(destName, name, reordered)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public EventFilter create(String name, NodeState before, NodeState after) {
                    List<EventFilter> childFilters = Lists.newArrayList();
                    for (EventFilter filter : filters) {
                        EventFilter childFilter = filter.create(name, before, after);
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

}
