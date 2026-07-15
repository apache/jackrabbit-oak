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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility methods for {@link java.util.List} conversions.
 */
public class ListUtils {

    private ListUtils() {
        // no instances for you
    }

    /**
     * Convert an iterable to a list. The returning list is mutable and supports all optional operations.
     *
     * @param iterable the iterable to convert
     * @param <T> the type of the elements
     * @return the list
     */
    @NotNull
    public static <T> List<T> toList(@NotNull final Iterable<? extends T> iterable) {
        Objects.requireNonNull(iterable);
        List<T> result = new ArrayList<>();
        iterable.forEach(result::add);
        return result;
    }

    /**
     * Convert an iterable to a {@link java.util.LinkedList}. The returning LinkedList is mutable and supports all optional operations.
     *
     * @param iterable the iterator to convert
     * @param <T> the type of the elements
     * @return the LinkedList
     */
    @NotNull
    public static <T> List<T> toLinkedList(final Iterable<T> iterable) {
        Objects.requireNonNull(iterable);
        List<T> result = new LinkedList<>();
        iterable.forEach(result::add);
        return result;
    }

    /**
     * Convert an iterator to a list. The returning list is mutable and supports all optional operations.
     *
     * @param iterator the iterator to convert
     * @param <T> the type of the elements
     * @return the list
     */
    @NotNull
    public static <T> List<T> toList(final Iterator<? extends T> iterator) {
        Objects.requireNonNull(iterator);
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    /**
     * Split a list into partitions of a given size.
     *
     * @param list the list to partition
     * @param n the size of partitions
     * @param <T> the type of the elements
     * @return a list of partitions. The resulting partitions aren’t a view of the main List, so any changes happening to the main List won’t affect the partitions.
     */
    @NotNull
    public static <T> List<List<T>> partitionList(final List<T> list, final int n) {
        Objects.requireNonNull(list);
        return IntStream.range(0, list.size())
                .filter(i -> i % n == 0)
                .mapToObj(i -> list.subList(i, Math.min(i + n, list.size())))
                .collect(Collectors.toList());
    }

    /**
     * Returns a new list containing the elements of the specified list in reverse order.
     *
     * @param <T> the type of elements in the list
     * @param l the list to be reversed, must not be null
     * @return a new list containing the elements of the specified list in reverse order
     * @throws NullPointerException if the list is null
     */
    @NotNull
    public static <T> List<T> reverse(final List<T> l) {
        Objects.requireNonNull(l);
        return IntStream.range(0, l.size()).map(i -> l.size() - 1 - i).mapToObj(l::get).collect(Collectors.toList());
    }
}
