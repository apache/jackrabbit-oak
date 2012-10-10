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

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConfigurationParameters... TODO
 */
public class ConfigurationParameters {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ConfigurationParameters.class);

    private final Map<String, Object> options;

    public ConfigurationParameters() {
        this(null);
    }

    public ConfigurationParameters(Map<String, ?> options) {
        this.options = (options == null) ? Collections.<String, Object>emptyMap() : Collections.unmodifiableMap(options);
    }

    public <T> T getConfigValue(String key, T defaultValue) {
        if (options != null && options.containsKey(key)) {
            return convert(options.get(key), defaultValue);
        } else {
            return defaultValue;
        }
    }

    //--------------------------------------------------------< private >---
    @SuppressWarnings("unchecked")
    private static <T> T convert(Object configProperty, T defaultValue) {
        T value;
        String str = configProperty.toString();
        Class targetClass = (defaultValue == null) ? configProperty.getClass() : defaultValue.getClass();
        try {
            if (targetClass == configProperty.getClass()) {
                value = (T) configProperty;
            } else if (targetClass == String.class) {
                value = (T) str;
            } else if (targetClass == Integer.class) {
                value = (T) Integer.valueOf(str);
            } else if (targetClass == Long.class) {
                value = (T) Long.valueOf(str);
            } else if (targetClass == Double.class) {
                value = (T) Double.valueOf(str);
            } else if (targetClass == Boolean.class) {
                value = (T) Boolean.valueOf(str);
            } else {
                // unsupported target type
                log.warn("Unsupported target type {} for value {}", targetClass.getName(), str);
                throw new IllegalArgumentException("Cannot convert config entry " + str + " to " + targetClass.getName());
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid value {}; cannot be parsed into {}", str, targetClass.getName());
            value = defaultValue;
        }
        return value;
    }
}