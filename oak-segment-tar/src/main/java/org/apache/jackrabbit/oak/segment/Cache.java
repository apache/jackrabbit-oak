/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.segment;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Partial mapping of keys of type {@code K} to values of type {@link V}. If supported by the
 * underlying implementation the mappings can further be associated with a cost, which is a
 * metric for the cost occurring when the given mapping is lost. Higher values represent higher
 * costs.
 */
public interface Cache<K, V> {

    /**
     * Add a mapping from {@code key} to {@code value}.
     * @throws UnsupportedOperationException   if the underlying implementation doesn't
     *         support values without an associated cost and {@link #put(Object, Object, byte)}
     *         should be used instead.
     */
    void put(@Nonnull K key, @Nonnull V value);

    /**
     * Add a mapping from {@code key} to {@code value} with a given {@code cost}.
     * @throws UnsupportedOperationException   if the underlying implementation doesn't
     *         support values with an associated cost and {@link #put(Object, Object)}
     *         should be used instead.
     */
    void put(@Nonnull K key, @Nonnull V value, byte cost);

    /**
     * @return  The mapping for {@code key}, or {@code null} if none.
     */
    @CheckForNull
    V get(@Nonnull K key);
}
