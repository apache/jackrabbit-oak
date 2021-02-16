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
package org.apache.jackrabbit.oak.commons.properties;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Utility class for consistent handling of system properties.
 * <p>
 * It provides for:
 * <ul>
 * <li>TRACE level logging of getting the system property
 * <li>ERROR level logging when value does not parse or is invalid (where
 * validity can be checked by a {@link Predicate})
 * <li>(default) INFO level logging when value differs from default (log level
 * and message format can be overridden)
 * </ul>
 * <p>
 * The supported types are: {@link Boolean}, {@link Integer},  {@link Long}, {@link String}
 */
public class SystemPropertySupplier<T> implements Supplier<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SystemPropertySupplier.class);

    private final String propName;
    private final T defaultValue;
    private final Function<String, T> parser;

    private Logger log = LOG;
    private Level successLogLevel = Level.INFO;
    private Predicate<T> validator = (a) -> true;
    private Function<String, String> sysPropReader = System::getProperty;
    private BiFunction<String, T, String> setMessageFormatter = (a, b) -> {
        return String.format("System property %s found to be '%s'", a, b);
    };

    private SystemPropertySupplier(@NotNull String propName, @NotNull T defaultValue) throws IllegalArgumentException {
        this.propName = Objects.requireNonNull(propName, "propertyName must be non-null");
        this.defaultValue = Objects.requireNonNull(defaultValue, "defaultValue must be non-null");
        this.parser = getValueParser(defaultValue);
    }

    /**
     * Create it for a given property name and default value.
     */
    public static <U> SystemPropertySupplier<U> create(@NotNull String propName, @NotNull U defaultValue)
            throws IllegalArgumentException {
        return new SystemPropertySupplier<U>(propName, defaultValue);
    }

    /**
     * Specify the {@link Logger} to log to (defaults to this classes logger otherwise).
     */
    public SystemPropertySupplier<T> loggingTo(@NotNull Logger log) {
        this.log = Objects.requireNonNull(log);
        return this;
    }

    /**
     * Specify a validation expression.
     */
    public SystemPropertySupplier<T> validateWith(@NotNull Predicate<T> validator) {
        this.validator = Objects.requireNonNull(validator);
        return this;
    }

    /**
     * Specify a formatter for the "success" log message to be used when the
     * returned property value differs from the default.
     */
    public SystemPropertySupplier<T> formatSetMessage(@NotNull BiFunction<String, T, String> setMessageFormatter) {
        this.setMessageFormatter = Objects.requireNonNull(setMessageFormatter);
        return this;
    }

    /**
     * Specify {@link Level} to use for "success" message.
     */
    public SystemPropertySupplier<T> logSuccessAs(Level successLogLevel) {
        this.successLogLevel = Objects.requireNonNull(successLogLevel);
        return this;
    }

    /**
     * <em>For unit testing</em>: specify a function to read system properties
     * (overriding default of {@code System.getProperty(String}).
     */
    protected SystemPropertySupplier<T> usingSystemPropertyReader(@NotNull Function<String, String> sysPropReader) {
        this.sysPropReader = Objects.requireNonNull(sysPropReader);
        return this;
    }

    /**
     * Obtains the value of a system property, optionally generating
     * diagnostics.
     * 
     * @return value of system property
     */
    public T get() {

        T ret = defaultValue;

        String value = sysPropReader.apply(propName);
        if (value == null) {
            log.trace("System property {} not set", propName);
        } else {
            log.trace("System property {} set to '{}'", propName, value);
            try {
                T v = parser.apply(value);
                if (!validator.test(v)) {
                    log.error("Ignoring invalid value '{}' for system property {}", value, propName);
                } else {
                    ret = v;
                }
            } catch (NumberFormatException ex) {
                log.error("Ignoring malformed value '{}' for system property {}", value, propName);
            }

            if (!ret.equals(defaultValue)) {
                String msg = setMessageFormatter.apply(propName, ret);
                switch (successLogLevel) {
                    case INFO:
                        log.info(msg);
                        break;
                    case DEBUG:
                        log.debug(msg);
                        break;
                    case ERROR:
                        log.error(msg);
                        break;
                    case TRACE:
                        log.trace(msg);
                        break;
                    case WARN:
                        log.warn(msg);
                        break;
                    default:
                        break;
                }
            }
        }

        return ret;
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<String, T> getValueParser(T defaultValue) {
        if (defaultValue instanceof Boolean) {
            return v -> (T) Boolean.valueOf(v);
        } else if (defaultValue instanceof Integer) {
            return v -> (T) Integer.valueOf(v);
        } else if (defaultValue instanceof Long) {
            return v -> (T) Long.valueOf(v);
        } else if (defaultValue instanceof String) {
            return v -> (T) v;
        } else {
            throw new IllegalArgumentException(
                    String.format("expects a defaultValue of Boolean, Integer, Long, or String, but got: %s", defaultValue.getClass()));
        }
    }
}
