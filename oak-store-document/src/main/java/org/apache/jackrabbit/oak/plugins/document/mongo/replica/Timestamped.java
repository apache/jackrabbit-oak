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
package org.apache.jackrabbit.oak.plugins.document.mongo.replica;

import com.google.common.base.Function;

/**
 * A value with a timestamp.
 *
 * @param <T> the value type
 */
public class Timestamped<T> {

    private final T value;

    private final long operationTimestamp;

    public Timestamped(T value, long operationTimestamp) {
        this.value = value;
        this.operationTimestamp = operationTimestamp;
    }

    public T getValue() {
        return value;
    }

    public long getOperationTimestamp() {
        return operationTimestamp;
    }

    public static <T> Function<Timestamped<T>, T> getExtractFunction() {
        return new Function<Timestamped<T>, T>() {
            @Override
            public T apply(Timestamped<T> input) {
                if (input == null) {
                    return null;
                } else {
                    return input.value;
                }
            }
        };
    }

    @Override
    public String toString() {
        return new StringBuilder("Timestamped[").append(value).append('(').append(operationTimestamp).append(")]")
                .toString();
    }
}