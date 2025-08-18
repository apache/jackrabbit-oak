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
package org.apache.jackrabbit.oak.commons.pconcurrent;

import java.util.function.Supplier;

/**
 * Utility methods for {@link java.util.function.Supplier} handling.
 */
public class Suppliers {

    private Suppliers() {
    }

    /**
     * Transforms a {@code Supplier} based on a wrapper around a lazily and only-tine
     * single time evaluation of the given {@code Supplier}.
     *
     * @param <T> return type
     * @return Supplier based on the given Supplier
     */
    public static <T> Supplier<T> memoize(final Supplier<T> computeOnce) {
        return new Supplier<>() {
            volatile boolean initialized = false;
            T result = null;

            @Override
            public T get() {
                if (!initialized) {
                    synchronized (this) {
                        if (!initialized) {
                            initialized = true;
                            result = computeOnce.get();
                        }
                    }
                }
                return result;
            }
        };
    }
}
