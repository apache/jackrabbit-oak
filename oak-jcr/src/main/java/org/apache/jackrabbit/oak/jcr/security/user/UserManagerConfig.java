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
package org.apache.jackrabbit.oak.jcr.security.user;

import org.apache.jackrabbit.oak.jcr.security.user.action.AuthorizableAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * UserManagerConfig...
 */
public class UserManagerConfig {

    private static final Logger log = LoggerFactory.getLogger(UserManagerImpl.class);

    /**
     * Configuration parameter to change the default algorithm used to generate
     * password hashes. The default value is {@link PasswordUtility#DEFAULT_ALGORITHM}.
     */
    public static final String PARAM_PASSWORD_HASH_ALGORITHM = "passwordHashAlgorithm";

    /**
     * Configuration parameter to change the number of iterations used for
     * password hash generation. The default value is {@link PasswordUtility#DEFAULT_ITERATIONS}.
     */
    public static final String PARAM_PASSWORD_HASH_ITERATIONS = "passwordHashIterations";

    /**
     * Configuration parameter to change the number of iterations used for
     * password hash generation. The default value is {@link PasswordUtility#DEFAULT_ITERATIONS}.
     */
    public static final String PARAM_PASSWORD_SALT_SIZE = "passwordSaltSize";

    private final Map<String, Object> config;
    private final String adminId;
    private final AuthorizableAction[] actions;

    UserManagerConfig(Map<String, Object> config, String adminId, AuthorizableAction[] actions) {
        this.config = config;
        this.adminId = adminId;
        this.actions = (actions == null) ? new AuthorizableAction[0] : actions;
    }

    public <T> T getConfigValue(String key, T defaultValue) {
        if (config != null && config.containsKey(key)) {
            return convert(config.get(key), defaultValue);
        } else {
            return defaultValue;
        }
    }

    public String getAdminId() {
        return adminId;
    }

    public AuthorizableAction[] getAuthorizableActions() {
        return actions;
    }

    //--------------------------------------------------------< private >---
    @SuppressWarnings("unchecked")
    private static <T> T convert(Object configProperty, T defaultValue) {
        T value;
        String str = configProperty.toString();
        Class targetClass = (defaultValue == null) ? String.class : defaultValue.getClass();
        try {
            if (targetClass == String.class) {
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