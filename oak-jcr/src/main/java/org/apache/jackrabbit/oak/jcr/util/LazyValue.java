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
package org.apache.jackrabbit.oak.jcr.util;

/**
 * Abstract base class for a lazy value of type {@code T}. The initialisation of
 * the actual value is deferred until first accessed.
 *
 * @param <T> type of the value
 */
public abstract class LazyValue<T> {
    private T value;

    /**
     * Create the value.
     * @return  the value
     */
    protected abstract T create();

    /**
     * Retrieve the value. The first call of this method results in a call
     * to {@link #create()}. Subsequent calls return the same instance as the
     * first call.
     *
     * @return the underlying value
     */
    public synchronized T get() {
        if (value == null) {
            value = create();
        }
        return value;
    }
}
