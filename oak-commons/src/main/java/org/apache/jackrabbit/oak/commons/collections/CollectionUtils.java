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

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Utility methods for collections conversions.
 */
public class CollectionUtils {

    private CollectionUtils() {
        // no instances for you
    }

    /**
     * Convert an iterable to a list.The retuning list is mutable and supports all optional operations.
     * @param iterable the iterable to convert
     * @return the list
     * @param <T> the type of the elements
     */
    public static <T> List<T> toList(final Iterable<T> iterable) {
        return IterableUtils.toList(iterable);
    }

    /**
     * Convert an iterator to a list. The retuning list is mutable and supports all optional operations.
     * @param iterator the iterator to convert
     * @return the list
     * @param <T> the type of the elements
     */
    public static <T> List<T> toList(final Iterator<T> iterator) {
        return IteratorUtils.toList(iterator);
    }

    /**
     * Convert an iterable to a set. The retuning set is mutable and supports all optional operations.
     * @param iterable the iterable to convert
     * @return the set
     * @param <T> the type of the elements
     */
    public static <T> Set<T> toSet(final Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toSet());
    }

    /**
     * Convert an iterator to a set. The retuning set is mutable and supports all optional operations.
     * @param iterator the iterator to convert
     * @return the set
     * @param <T> the type of the elements
     */
    public static <T> Set<T> toSet(final Iterator<T> iterator) {
        final Set<T> set = new HashSet<>();
        iterator.forEachRemaining(set::add);
        return set;
    }

}
