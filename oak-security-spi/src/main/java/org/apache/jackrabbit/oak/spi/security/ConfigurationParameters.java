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
package org.apache.jackrabbit.oak.spi.security;

import java.util.Collection;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConfigurationParameters is a convenience class that allows typed access to configuration properties. It implements
 * the {@link Map} interface but is immutable.
 */
public final class ConfigurationParameters implements Map<String, Object> {

    /**
     * internal logger
     */
    private static final Logger log = LoggerFactory.getLogger(ConfigurationParameters.class);

    /**
     * An empty configuration parameters
     */
    public static final ConfigurationParameters EMPTY = new ConfigurationParameters();

    /**
     * internal map of the config parameters
     */
    private final Map<String, Object> options;

    /**
     * creates an empty config parameters instance.
     * Note: the constructor is private to avoid creation of empty maps.
     */
    private ConfigurationParameters() {
        this.options = Collections.emptyMap();
    }

    /**
     * Creates an config parameter instance.
     * Note: the constructor is private to avoid creation of empty maps.
     * @param options the source options.
     */
    private ConfigurationParameters(@NotNull Map<String, ?> options) {
        this.options = Collections.unmodifiableMap(options);
    }

    /**
     * Creates a new configuration parameters instance by merging all {@code params} sequentially.
     * I.e. property define in subsequent arguments overwrite the ones before.
     *
     * @param params source parameters to merge
     * @return merged configuration parameters or {@link #EMPTY} if all source params were empty.
     */
    @NotNull
    public static ConfigurationParameters of(@NotNull ConfigurationParameters... params) {
        Map<String, Object> m = new HashMap<>();
        for (ConfigurationParameters cp : params) {
            if (cp != null) {
                m.putAll(cp.options);
            }
        }
        return m.isEmpty() ? EMPTY : new ConfigurationParameters(m);
    }

    /**
     * Creates new a configuration parameters instance by copying the given properties.
     * @param properties source properties
     * @return configuration parameters or {@link #EMPTY} if the source properties were empty.
     */
    @NotNull
    public static ConfigurationParameters of(@NotNull Properties properties) {
        if (properties.isEmpty()) {
            return EMPTY;
        }
        Map<String, Object> options = new HashMap<>(properties.size());
        for (Object name : properties.keySet()) {
            final String key = name.toString();
            options.put(key, properties.get(key));
        }
        return new ConfigurationParameters(options);
    }

    /**
     * Creates new a configuration parameters instance by copying the given properties.
     * @param properties source properties
     * @return configuration parameters or {@link #EMPTY} if the source properties were empty.
     */
    @NotNull
    public static ConfigurationParameters of(@NotNull Dictionary<String, Object> properties) {
        if (properties.isEmpty()) {
            return EMPTY;
        }
        Map<String, Object> options = new HashMap<>(properties.size());
        for (Enumeration<String> keys = properties.keys(); keys.hasMoreElements();) {
            String key = keys.nextElement();
            options.put(key, properties.get(key));
        }
        return new ConfigurationParameters(options);
    }

    /**
     * Creates new a configuration parameters instance by copying the given map.
     * @param map source map
     * @return configuration parameters or {@link #EMPTY} if the source map was empty.
     */
    @NotNull
    public static ConfigurationParameters of(@NotNull Map<?, ?> map) {
        if (map.isEmpty()) {
            return EMPTY;
        }
        if (map instanceof ConfigurationParameters) {
            return (ConfigurationParameters) map;
        }
        Map<String, Object> options = new HashMap<>(map.size());
        map.forEach((key, value) -> options.put(String.valueOf(key), value));
        return new ConfigurationParameters(options);
    }

    /**
     * Creates new a single valued configuration parameters instance from the
     * given key and value.
     *
     * @param key The key
     * @param value The value
     * @return a new instance of configuration parameters.
     */
    @NotNull
    public static ConfigurationParameters of(@NotNull String key, @NotNull Object value) {
        return new ConfigurationParameters(Map.of(key, value));
    }

    /**
     * Creates new a configuration parameters instance from the
     * given key and value pairs.
     *
     * @param key1 The key of the first pair.
     * @param value1 The value of the first pair
     * @param key2 The key of the second pair.
     * @param value2 The value of the second pair.
     * @return a new instance of configuration parameters.
     */
    @NotNull
    public static ConfigurationParameters of(@NotNull String key1, @NotNull Object value1,
                                             @NotNull String key2, @NotNull Object value2) {
        return new ConfigurationParameters(Map.of(key1, value1, key2, value2));
    }

    /**
     * Returns {@code true} if this instance contains a configuration entry with
     * the specified key irrespective of the defined value; {@code false} otherwise.
     *
     * @param key The key to be tested.
     * @return {@code true} if this instance contains a configuration entry with
     * the specified key irrespective of the defined value; {@code false} otherwise.
     */
    public boolean contains(@NotNull String key) {
        return options.containsKey(key);
    }

    /**
     * Returns the value of the configuration entry with the given {@code key}
     * applying the following rules:
     *
     * <ul>
     *     <li>If this instance doesn't contain a configuration entry with that
     *     key the specified {@code defaultValue} will be returned.</li>
     *     <li>If {@code defaultValue} is {@code null} the original value will
     *     be returned.</li>
     *     <li>If the configured value is {@code null} this method will always
     *     return {@code null}.</li>
     *     <li>If neither {@code defaultValue} nor the configured value is
     *     {@code null} an attempt is made to convert the configured value to
     *     match the type of the default value.</li>
     * </ul>
     *
     * @param key The name of the configuration option.
     * @param defaultValue The default value to return if no such entry exists
     * or to use for conversion.
     * @param targetClass The target class
     * @return The original or converted configuration value or {@code null}.
     */
    @Nullable
    public <T> T getConfigValue(@NotNull String key, @Nullable T defaultValue,
                                @Nullable Class<T> targetClass) {
        if (options.containsKey(key)) {
            Object property = options.get(key);
            return (property == null) ? null : convert(property, getTargetClass(property, defaultValue, targetClass));
        } else {
            return defaultValue;
        }
    }

    /**
     * Returns the value of the configuration entry with the given {@code key}
     * applying the following rules:
     *
     * <ul>
     *     <li>If this instance doesn't contain a configuration entry with that
     *     key, or if the entry is {@code null}, the specified {@code defaultValue} will be returned.</li>
     *     <li>If the configured value is not {@code null} an attempt is made to convert the configured value to
     *     match the type of the default value.</li>
     * </ul>
     *
     * @param key The name of the configuration option.
     * @param defaultValue The default value to return if no such entry exists
     * or to use for conversion.
     * @return The original or converted configuration value or {@code defaultValue} if no entry for the given key exists.
     */
    @NotNull
    public <T> T getConfigValue(@NotNull String key, @NotNull T defaultValue) {
        Object property = options.get(key);
        if (property == null) {
            return defaultValue;
        } else {
            T value = convert(property, getTargetClass(property, defaultValue, null));
            return (value == null) ? defaultValue : value;
        }
    }

    //--------------------------------------------------------< private >---
    @NotNull
    private static Class<?>  getTargetClass(@NotNull Object configProperty, @Nullable Object defaultValue, @Nullable Class<?> targetClass) {
        Class<?> clazz = targetClass;
        if (clazz == null) {
            clazz = (defaultValue == null)
                    ? configProperty.getClass()
                    : defaultValue.getClass();
        }
        return clazz;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private static <T> T convert(@NotNull Object configProperty, @NotNull Class<?> clazz) {
        String str = configProperty.toString();
        if (clazz.isAssignableFrom(configProperty.getClass())) {
            return (T) configProperty;
        } else if (clazz == String.class) {
            return (T) str;
        } else if (clazz == Milliseconds.class) {
            Milliseconds ret = Milliseconds.of(str);
            return ret == null ? null : (T) ret;
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return (T) Boolean.valueOf(str);
        } else if (clazz == String[].class){
            return (T) PropertiesUtil.toStringArray(configProperty);
        } else if (clazz == Set.class || Set.class.isAssignableFrom(clazz)) {
            return (T) convertToSet(configProperty, clazz);
        } else {
            return (T) convertToNumber(str, clazz);
        }
    }
    
    @NotNull
    private static Object convertToNumber(@NotNull String str, @NotNull Class<?> clazz) {
        try {
            if (clazz == Integer.class || clazz == int.class) {
                return Integer.valueOf(str);
            } else if (clazz == Long.class || clazz == long.class) {
                return Long.valueOf(str);
            } else if (clazz == Float.class || clazz == float.class) {
                return Float.valueOf(str);
            } else if (clazz == Double.class || clazz == double.class) {
                return Double.valueOf(str);
            } else {
                // unsupported target type
                log.warn("Unsupported target type {} for value {}", clazz.getName(), str);
                throw conversionFailedException(str, clazz.getName(), null);
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid value {}; cannot be parsed into {}", str, clazz.getName());
            throw conversionFailedException(str, clazz.getName(), e);
        }
    }
    
    @NotNull
    private static Set<?> convertToSet(@NotNull Object configProperty, @NotNull Class<?> clazz) {
        if (configProperty instanceof Set) {
            return (Set) configProperty;
        } else if (configProperty instanceof Collection<?>) {
            return ImmutableSet.copyOf((Collection<?>) configProperty);
        } else if (configProperty.getClass().isArray()) {
            return ImmutableSet.copyOf((Object[]) configProperty);
        } else {
            String[] arr = PropertiesUtil.toStringArray(configProperty);
            if (arr != null) {
                return ImmutableSet.copyOf(arr);
            } else {
                String str = configProperty.toString();
                log.warn("Unsupported target type {} for value {}", clazz.getName(), str);
                throw conversionFailedException(str, clazz.getName(), null);
            }
        }
    }
    
    @NotNull
    private static IllegalArgumentException conversionFailedException(@NotNull String str, @NotNull String className, @Nullable Exception e) {
        return new IllegalArgumentException("Cannot convert config entry " + str + " to " + className, e);
    }

    //-------------------------------------------< Map interface delegation >---
    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return options.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return options.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key) {
        return options.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
        return options.containsValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object get(Object key) {
        return options.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(@NotNull Map<? extends String, ?> m) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public Set<String> keySet() {
        return options.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public Collection<Object> values() {
        return options.values();
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public Set<Entry<String,Object>> entrySet() {
        return options.entrySet();
    }

    /**
     * Helper class for configuration parameters that denote a "duration", such
     * as a timeout or expiration time.
     */
    public static final class Milliseconds {

        private static final Pattern pattern = Pattern.compile("(\\d+)(\\.\\d+)?(ms|s|m|h|d)?");

        public static final Milliseconds NULL = new Milliseconds(0);

        public static final Milliseconds FOREVER = new Milliseconds(Long.MAX_VALUE);

        public static final Milliseconds NEVER = new Milliseconds(-1);

        public final long value;

        private Milliseconds(long value) {
            this.value = value;
        }

        /**
         * Returns a new milliseconds object from the given long value.
         *
         * @param value the value
         * @return the milliseconds object
         */
        public static Milliseconds of(long value) {
            if (value == 0) {
                return NULL;
            } else if (value == Long.MAX_VALUE) {
                return FOREVER;
            } else if (value < 0) {
                return NEVER;
            } else {
                return new Milliseconds(value);
            }
        }

        /**
         * Parses a value string into a duration. the String has the following format:
         * {@code
         * <xmp>
         *     format:= (value [ unit ])+;
         *     value:= float value;
         *     unit: "ms" | "s" | "m" | "h" | "d";
         * </xmp>
         *
         * Example:
         * <xmp>
         *     "100", "100ms" : 100 milliseconds
         *     "1s 50ms": 1050 milliseconds
         *     "1.5d":  1 1/2 days == 36 hours.
         * </xmp>
         * }
         *
         * @param str the string to parse
         * @return the new Milliseconds object or null.
         */
        @Nullable
        public static Milliseconds of(@Nullable String str) {
            if (str == null) {
                return null;
            }
            Matcher m = pattern.matcher(str);
            long current = -1;
            while (m.find()) {
                String number = m.group(1);
                String decimal = m.group(2);
                if (decimal != null) {
                    number += decimal;
                }
                String unit = m.group(3);
                double value = Double.parseDouble(number);
                if ("s".equals(unit)) {
                    value *= 1000.0;
                } else if ("m".equals(unit)) {
                    value *= 60 * 1000.0;
                } else if ("h".equals(unit)) {
                    value *= 60 * 60 * 1000.0;
                } else if ("d".equals(unit)) {
                    value *= 24 * 60 * 60 * 1000.0;
                }
                current += value;
            }
            return current < 0 ? null : new Milliseconds(current + 1);
        }

        @NotNull
        public static Milliseconds of(@Nullable String str, @NotNull Milliseconds defaultValue) {
            if (str == null) {
                return defaultValue;
            }
            Milliseconds ms = of(str);
            return (ms == null) ? defaultValue : ms;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || !(o == null || getClass() != o.getClass()) && value == ((Milliseconds) o).value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }
}
