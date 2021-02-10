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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * @deprecated please use org.apache.jackrabbit.oak.commons.SystemPropertySupplier
 * @see org.apache.jackrabbit.oak.commons.SystemPropertySupplier
 */
public class SystemPropertySupplier<T> implements Supplier<T> {

    public static <U> SystemPropertySupplier<U> create(@NotNull String propName, @NotNull U defaultValue)
            throws IllegalArgumentException {
        return new SystemPropertySupplier<U>(propName, defaultValue);
    }

    private final org.apache.jackrabbit.oak.commons.SystemPropertySupplier<T> delegate;

    private SystemPropertySupplier(@NotNull String propName, @NotNull T defaultValue) throws IllegalArgumentException {
        delegate = org.apache.jackrabbit.oak.commons.SystemPropertySupplier.create(propName, defaultValue);
    }

    @Override
    public T get() {
        return delegate.get();
    }

    public SystemPropertySupplier<T> loggingTo(@NotNull Logger log) {
        delegate.loggingTo(log);
        return this;
    }

    public SystemPropertySupplier<T> validateWith(@NotNull Predicate<T> validator) {
        delegate.validateWith(validator);
        return this;
    }

    public SystemPropertySupplier<T> formatSetMessage(@NotNull BiFunction<String, T, String> setMessageFormatter) {
        delegate.formatSetMessage(setMessageFormatter);
        return this;
    }

    public SystemPropertySupplier<T> logSuccessAs(Level successLogLevel) {
        delegate.logSuccessAs(successLogLevel);
        return this;
    }
}
