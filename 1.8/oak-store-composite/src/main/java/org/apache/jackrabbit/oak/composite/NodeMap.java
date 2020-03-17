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
package org.apache.jackrabbit.oak.composite;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.spi.mount.Mount;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class NodeMap<T> {

    private final Map<MountedNodeStore, CacheableSupplier<T>> suppliers;

    private NodeMap(Map<MountedNodeStore, CacheableSupplier<T>> suppliers) {
        this.suppliers = suppliers;
    }

    public static <T> NodeMap<T> create(Map<MountedNodeStore, T> nodes) {
        ImmutableMap.Builder<MountedNodeStore, CacheableSupplier<T>> suppliers = ImmutableMap.builder();
        nodes.forEach((mns, node) -> suppliers.put(mns, new CacheableSupplier<T>(node)));
        return new NodeMap<>(suppliers.build());
    }

    public T get(MountedNodeStore nodeStore) {
        if (!suppliers.containsKey(nodeStore)) {
            Mount mount = nodeStore.getMount();
            String mountName = mount.isDefault() ? "[default]" : mount.getName();
            throw new IllegalStateException("Node is not available for the node store " + mountName);
        }
        return suppliers.get(nodeStore).get();
    }

    public <R> NodeMap<R> getAndApply(BiFunction<MountedNodeStore, T, R> function) {
        ImmutableMap.Builder<MountedNodeStore, CacheableSupplier<R>> newSuppliers = ImmutableMap.builder();
        suppliers.forEach((mns, node) -> newSuppliers.put(mns, node.getAndApply(curry(function, mns))));
        return new NodeMap<>(newSuppliers.build());
    }

    public <R> NodeMap<R> lazyApply(BiFunction<MountedNodeStore, T, R> function) {
        ImmutableMap.Builder<MountedNodeStore, CacheableSupplier<R>> newSuppliers = ImmutableMap.builder();
        suppliers.forEach((mns, node) -> newSuppliers.put(mns, node.lazyApply(curry(function, mns))));
        return new NodeMap<>(newSuppliers.build());
    }

    private static <T, U, R> Function<U, R> curry(BiFunction<T, U, R> function, T value) {
        return u -> function.apply(value, u);
    }

    public NodeMap<T> replaceNode(MountedNodeStore nodeStore, T node) {
        ImmutableMap.Builder<MountedNodeStore, CacheableSupplier<T>> newSuppliers = ImmutableMap.builder();
        suppliers.forEach((mns, n) -> {
            if (mns != nodeStore) {
                newSuppliers.put(mns, n);
            }
        });
        newSuppliers.put(nodeStore, new CacheableSupplier<>(node));
        return new NodeMap<>(newSuppliers.build());
    }

    private static class CacheableSupplier<T> implements Supplier<T> {

        private Supplier<T> supplier;

        private volatile T value;

        private CacheableSupplier(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        public CacheableSupplier(T value) {
            this.value = value;
        }

        @Override
        public T get() {
            if (value == null) {
                synchronized (this) {
                    if (value == null) {
                        value = supplier.get();
                        supplier = null;
                    }
                }
            }
            return value;
        }

        public <R> CacheableSupplier<R> getAndApply(Function<T, R> function) {
            return new CacheableSupplier<>(function.apply(get()));
        }

        public <R> CacheableSupplier<R> lazyApply(Function<T, R> function) {
            return new CacheableSupplier<>(() -> function.apply(get()));
        }
    }
}
