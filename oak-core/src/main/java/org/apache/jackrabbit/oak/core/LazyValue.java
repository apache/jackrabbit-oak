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
package org.apache.jackrabbit.oak.core;

/**
 * An instances of this class represents a lazy value of type {@code T}.
 * {@code LazyValue} implements an evaluate by need semantics:
 * {@link #createValue()} is called exactly once when {@link #get()}
 * is called for the first time.
 * <p>
 * {@code LazyValue} instances are thread safe.
 */
abstract class LazyValue<T> {
    private volatile T value;

    /**
     * Factory method called to create the value on an as need basis.
     * @return a new instance for {@code T}.
     */
    protected abstract T createValue();

    /**
     * @return  {@code true} iff {@link #get()} has been called at least once.
     */
    public boolean hasValue() {
        return value != null;
    }

    /**
     * Get value. Calls {@link #createValue()} if called for the first time.
     * @return  the value
     */
    public T get () {
        // Double checked locking is fine since Java 5 as long as value is volatile.
        // See http://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html
        if (value == null) {
            synchronized (this) {
                if (value == null) {
                    value = createValue();
                }
            }
        }
        return value;
    }

}
