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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a counted reference to a closeable object.
 * <p>
 * This class enables reference counting for an instance. The instance should
 * always be accessed through a reference ({@link #get()}), which guarantees
 * that the underlying instance remains live during its usage.
 * <p>
 * When a reference is not needed, it should be closed. If the closed reference
 * is the last reference to the underlying object, the the underlying object is
 * closed. If other references to the underlying object are around, the object
 * is kept alive.
 * <p>
 * Given a reference to an object, it is possible to create a new reference to
 * it using the {@link #reference()} method.
 * <p>
 * References are thread safe. This allows to safely share an object between
 * multiple threads. Every thread gets its own reference to the same underlying
 * object, and closes it when the reference is not needed anymore. The
 * underlying object will automatically be closed by the last thread using the
 * resource.
 *
 * @param <T> The type of the underlying object.
 */
class Reference<T extends Closeable> implements Closeable {

    /**
     * Create a new reference to an underlying object.
     *
     * @param instance The underlying object.
     * @param <T>      The type of the underlying object.
     * @return An instance of {@link Reference}.
     */
    static <T extends Closeable> Reference<T> of(final T instance) {
        return new Reference<>(checkNotNull(instance));
    }

    private final AtomicInteger references = new AtomicInteger(1);

    private final T instance;

    private Reference(T instance) {
        this.instance = instance;
    }

    /**
     * Get the underlying object referenced from this instance.
     *
     * @return The underlying object.
     */
    public T get() {
        return instance;
    }

    /**
     * Create a new reference to the underlying object.
     * <p>
     * The instance must be explicitly closed for the underlying object to be
     * closed. Failing to close all the references prevents the underlying
     * object from being closed.
     *
     * @return An instance of {@link Reference}.
     */
    public Reference<T> reference() {
        references.incrementAndGet();
        return this;
    }

    /**
     * Disposes this reference. If this instance is the last reference to the
     * underlying object, the underlying object will be disposed of as well.
     *
     * @throws IOException If an error occurs when closing the underlying
     *                     object.
     */
    public void close() throws IOException {
        if (references.decrementAndGet() == 0) {
            instance.close();
        }
    }

}
