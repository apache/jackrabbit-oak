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

package org.apache.jackrabbit.oak.spi.descriptors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.Descriptors;

/**
 * Default implementation of the {@link Descriptors} interface. Supports overlying of given default descriptors.
 */
public class GenericDescriptors implements org.apache.jackrabbit.oak.api.Descriptors {

    private final Descriptors base;

    private final Map<String, Descriptor> descriptors = new ConcurrentHashMap<String, Descriptor>();

    /**
     * Constructs an empty descriptors set.
     */
    public GenericDescriptors() {
        base = null;
    }

    /**
     * Constructs a descriptors set that uses the given {@code base} descriptors as base.
     * @param base the base descriptors or {@code null}
     */
    public GenericDescriptors(@Nullable Descriptors base) {
        this.base = base;
    }

    /**
     * {@inheritDoc}
     *
     * Note: If this descriptors are based on default descriptors, the returns string array is a merge of this and the
     * base's keys.
     */
    @Nonnull
    @Override
    public String[] getKeys() {
        Set<String> keys = new HashSet<String>(descriptors.keySet());
        if (base != null) {
            Collections.addAll(keys, base.getKeys());
        }
        return keys.toArray(new String[keys.size()]);
    }

    /**
     * {@inheritDoc}
     *
     * Note: If the descriptor with {@code key} does not exist in this set, the call is delegated to the base descriptors.
     */
    @Override
    public boolean isStandardDescriptor(@Nonnull String key) {
        return descriptors.containsKey(key) && descriptors.get(key).standard
                || base != null && base.isStandardDescriptor(key);
    }

    /**
     * {@inheritDoc}
     *
     * Note: If the descriptor with {@code key} does not exist in this set, the call is delegated to the base descriptors.
     */
    @Override
    public boolean isSingleValueDescriptor(@Nonnull String key) {
        return descriptors.containsKey(key) && descriptors.get(key).singleValued
                || base != null && base.isSingleValueDescriptor(key);
    }

    /**
     * {@inheritDoc}
     *
     * Note: If the descriptor with {@code key} does not exist in this set, the call is delegated to the base descriptors.
     */
    @CheckForNull
    @Override
    public Value getValue(@Nonnull String key) {
        Descriptor d = descriptors.get(key);
        if (d == null) {
            return base == null ? null : base.getValue(key);
        }
        return !d.singleValued ? null : d.values[0];
    }

    /**
     * {@inheritDoc}
     *
     * Note: If the descriptor with {@code key} does not exist in this set, the call is delegated to the base descriptors.
     */
    @CheckForNull
    @Override
    public Value[] getValues(@Nonnull String key) {
        Descriptor d = descriptors.get(key);
        if (d == null) {
            return base == null ? null : base.getValues(key);
        }
        return d.values;
    }

    /**
     * Adds a new descriptor to this set of descriptors which overlay the ones from the base set.
     *
     * @param name descriptor name
     * @param values array of descriptor values
     * @param singleValued flag indicating if this is single valued descriptor. see {@link Descriptors#isSingleValueDescriptor(String)}
     * @param standard flag indicating if this is a standard descriptor. see {@link Descriptors#isStandardDescriptor(String)}
     * @return {@code this} suitable for chaining.
     */
    public GenericDescriptors put(@Nonnull String name, @Nonnull Value[] values, boolean singleValued, boolean standard) {
        descriptors.put(name, new Descriptor(name, values, singleValued, standard));
        return this;
    }

    /**
     * Adds a new descriptor to this set of descriptors which overlay the ones from the base set.
     *
     * @param name descriptor name
     * @param value descriptor value
     * @param singleValued flag indicating if this is single valued descriptor. see {@link Descriptors#isSingleValueDescriptor(String)}
     * @param standard flag indicating if this is a standard descriptor. see {@link Descriptors#isStandardDescriptor(String)}
     * @return {@code this} suitable for chaining.
     */
    public GenericDescriptors put(@Nonnull String name, @Nonnull Value value, boolean singleValued, boolean standard) {
        descriptors.put(name, new Descriptor(name, new Value[]{value}, singleValued, standard));
        return this;
    }

    /**
     * Internal Descriptor class
     */
    private static final class Descriptor {

        final String name;
        final Value[] values;
        final boolean singleValued;
        final boolean standard;

        public Descriptor(String name, Value[] values, boolean singleValued, boolean standard) {
            this.name = name;
            this.values = values;
            this.singleValued = singleValued;
            this.standard = standard;
        }
    }

}
