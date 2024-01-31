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

package org.apache.jackrabbit.oak.commons.guava;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * A limited set of facade factory methods over the {@code org.apache.jackrabbit.guava.common.collect} API.
 */
public final class Collect {
    private Collect() {
    }

    /**
     * Returns the result of {@code Maps.newHashMap()}.
     *
     * @param <K> key type
     * @param <V> value type
     * @return the result of {@code Maps.newHashMap()}.
     * @see org.apache.jackrabbit.guava.common.collect.Maps#newHashMap()
     */
    public static <K, V> HashMap<K, V> newHashMap() {
        return Maps.newHashMap();
    }

    /**
     * Returns the result of {@code Sets.newHashSet()}.
     *
     * @param <E> element type
     * @return the result of {@code Sets.newHashSet()}.
     * @see org.apache.jackrabbit.guava.common.collect.Sets#newHashSet()
     */
    public static <E extends @Nullable Object> HashSet<E> newHashSet() {
        return Sets.newHashSet();
    }

    /**
     * Returns the result of {@code ImmutableMap.copyOf(map)}.
     *
     * @param map the mappings to be placed in the new map
     * @param <K> key type
     * @param <V> value type
     * @return the result of {@code ImmutableMap.copyOf(map)}
     * @see org.apache.jackrabbit.guava.common.collect.ImmutableMap#copyOf(java.util.Map)
     */
    public static <K, V> Map<K, V> immutableMapCopyOf(Map<? extends K, ? extends V> map) {
        return ImmutableMap.copyOf(map);
    }
}
