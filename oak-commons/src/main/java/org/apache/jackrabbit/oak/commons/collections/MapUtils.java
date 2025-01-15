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
package org.apache.jackrabbit.oak.commons.collections;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utility methods for {@link java.util.Map} conversions.
 */
public class MapUtils {

    private MapUtils() {
        // no instances for you
    }

    /**
     * Creates a new, empty HashMap with expected capacity.
     * <p>
     * The returned map uses the default load factor of 0.75, and its capacity is
     * large enough to add expected number of elements without resizing.
     *
     * @param capacity the expected number of elements
     * @throws IllegalArgumentException if capacity is negative
     * @see SetUtils#newHashSet(int)
     * @see SetUtils#newLinkedHashSet(int)
     */
    @NotNull
    public static <K, V> Map<K, V> newHashMap(final int capacity) {
        // make sure the Map does not need to be resized given the initial content
        return new HashMap<>(CollectionUtils.ensureCapacity(capacity));
    }

    /**
     * Converts a {@link Properties} object to an unmodifiable {@link Map} with
     * string keys and values.
     *
     * @param properties the {@link Properties} object to convert, must not be null
     * @return an unmodifiable {@link Map} containing the same entries as the given {@link Properties} object
     * @throws NullPointerException if the properties parameter is null
     */
    @NotNull
    public static Map<String, String> fromProperties(final @NotNull Properties properties) {
        Objects.requireNonNull(properties);
        return properties.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue())));
    }

    /**
     * Create a new {@link Map} after filtering the entries of the given map
     * based on the specified predicate applied to the keys.
     *
     * @param <K> the type of keys in the map
     * @param <V> the type of values in the map
     * @param map the map to filter, must not be null
     * @param predicate the predicate to apply to the keys, must not be null
     * @return a new map containing only the entries whose keys match the predicate
     * @throws NullPointerException if the map or predicate is null
     * @see MapUtils#filterValues(Map, Predicate)
     * @see MapUtils#filterEntries(Map, Predicate)
     */
    @NotNull
    public static <K,V> Map<K, V> filterKeys(final @NotNull Map<K, V> map, final @NotNull Predicate<? super K> predicate) {
        Objects.requireNonNull(map);
        Objects.requireNonNull(predicate);
        return map.entrySet()
                .stream()
                .filter(e -> predicate.test(e.getKey())) // using LinkedHashMap to maintain the order of previous map
                .collect(LinkedHashMap::new, (m, e)->m.put(e.getKey(), e.getValue()), LinkedHashMap::putAll);
    }

    /**
     * Create a new {@link Map} after filtering the entries of the given map
     * based on the specified predicate applied to the values.
     *
     * @param <K> the type of keys in the map
     * @param <V> the type of values in the map
     * @param map the map to filter, must not be null
     * @param predicate the predicate to apply to the values, must not be null
     * @return a new map containing only the entries whose values match the predicate
     * @throws NullPointerException if the map or predicate is null
     * @see MapUtils#filterKeys(Map, Predicate)
     * @see MapUtils#filterEntries(Map, Predicate)
     */
    @NotNull
    public static <K,V> Map<K, V> filterValues(final @NotNull Map<K, V> map, final @NotNull Predicate<? super V> predicate) {
        Objects.requireNonNull(map);
        Objects.requireNonNull(predicate);
        return map.entrySet()
                .stream()
                .filter(e -> predicate.test(e.getValue())) // using LinkedHashMap to maintain the order of previous map
                .collect(LinkedHashMap::new, (m,e)->m.put(e.getKey(), e.getValue()), LinkedHashMap::putAll);
    }

    /**
     * Create a new {@link Map} after filtering the entries of the given map
     * based on the specified predicate applied to the {@link Map.Entry}.
     *
     * @param <K> the type of keys in the map
     * @param <V> the type of values in the map
     * @param map the map to filter, must not be null
     * @param predicate the predicate to apply to the {@link Map.Entry}, must not be null
     * @return a new map containing only the entries whose values match the predicate
     * @throws NullPointerException if the map or predicate is null
     * @see MapUtils#filterKeys(Map, Predicate)
     * @see MapUtils#filterValues(Map, Predicate)
     */
    @NotNull
    public static <K,V> Map<K, V> filterEntries(final @NotNull Map<K, V> map, final @NotNull Predicate<? super Map.Entry<K, V>> predicate) {
        Objects.requireNonNull(map);
        Objects.requireNonNull(predicate);
        return map.entrySet()
                .stream()
                .filter(predicate) // using LinkedHashMap to maintain the order of previous map
                .collect(LinkedHashMap::new, (m,e)->m.put(e.getKey(), e.getValue()), LinkedHashMap::putAll);
    }
}
