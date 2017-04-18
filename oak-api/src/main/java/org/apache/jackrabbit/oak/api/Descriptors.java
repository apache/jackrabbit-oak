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
 */
package org.apache.jackrabbit.oak.api;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Value;

/**
 * Repository descriptors interface that is used to support providing the repository descriptors of
 * {@link javax.jcr.Repository}
 */
public interface Descriptors {

    /**
     * Returns a string array holding all descriptor keys available for this
     * implementation, both the standard descriptors defined by the string
     * constants in this interface and any implementation-specific descriptors.
     * Used in conjunction with {@link #getValue(String key)} and
     * {@link #getValues(String key)} to query information about this
     * repository implementation.
     *
     * @return a string array holding all descriptor keys.
     */
    @Nonnull
    String[] getKeys();

    /**
     * Returns {@code true} if {@code key} is a standard descriptor
     * defined by the string constants in this interface and {@code false}
     * if it is either a valid implementation-specific key or not a valid key.
     *
     * @param key a descriptor key.
     * @return whether {@code key} is a standard descriptor.
     */
    boolean isStandardDescriptor(@Nonnull String key);

    /**
     * Returns {@code true} if {@code key} is a valid single-value
     * descriptor; otherwise returns {@code false}
     *
     * @param key a descriptor key.
     * @return whether the specified descriptor is multi-valued.
     * @since JCR 2.0
     */
    boolean isSingleValueDescriptor(@Nonnull String key);

    /**
     * The value of a single-value descriptor is found by passing the key for
     * that descriptor to this method. If {@code key} is the key of a
     * multi-value descriptor or not a valid key this method returns
     * {@code null}.
     *
     * @param key a descriptor key.
     * @return The value of the indicated descriptor
     */
    @CheckForNull
    Value getValue(@Nonnull String key);

    /**
     * The value array of a multi-value descriptor is found by passing the key
     * for that descriptor to this method. If {@code key} is the key of a
     * single-value descriptor then this method returns that value as an array
     * of size one. If {@code key} is not a valid key this method returns
     * {@code null}.
     *
     * @param key a descriptor key.
     * @return the value array for the indicated descriptor
     */
    @CheckForNull
    Value[] getValues(@Nonnull String key);
}