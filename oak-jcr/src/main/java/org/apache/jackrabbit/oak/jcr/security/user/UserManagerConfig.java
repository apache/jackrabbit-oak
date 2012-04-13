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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

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

    // TODO: check if that can really be node, who would retrieve it and what kind of access rights needed to be enforced on it
    private final Node configNode;
    private final String adminId;
    //private final AuthorizableAction[] actions;

    UserManagerConfig(Node configNode, String adminId) {
        this.configNode = configNode;
        this.adminId = adminId;
        // this.actions = (actions == null) ? new AuthorizableAction[0] : actions;
    }

    public <T> T getConfigValue(String key, T defaultValue) {
        try {
            if (configNode.hasProperty(key)) {
                return convert(configNode.getProperty(key), defaultValue);
            }
        } catch (RepositoryException e) {
            // unexpected error -> return default value
            log.debug(e.getMessage());
        }
        return defaultValue;
    }

    public String getAdminId() {
        return adminId;
    }

//    public AuthorizableAction[] getAuthorizableActions() {
//        return actions;
//    }

    //--------------------------------------------------------< private >---
    private static <T> T convert(Property configProperty, T defaultValue) throws RepositoryException {
        T value;
        String str;
        // TODO properly deal with multi-value properties and array-default-values.
        if (configProperty.isMultiple()) {
            Value[] vls = configProperty.getValues();
            str = (vls.length == 0) ? "" : vls[0].getString();
        } else {
            str = configProperty.getString();
        }
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
                log.warn("Unsupported target type {} for config entry {}", targetClass.getName(), configProperty.getName());
                throw new IllegalArgumentException("Cannot convert config entry " + configProperty.getName() + " to " + targetClass.getName());
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid value of config entry {}; cannot be parsed into {}", configProperty.getName(), targetClass.getName());
            value = defaultValue;
        }
        return value;
    }
}
