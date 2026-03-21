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
package org.apache.jackrabbit.core.config;

import javax.jcr.RepositoryException;

/**
 * Exception class used for configuration errors.
 */
public class ConfigurationException extends RepositoryException {

    /**
     * Creates a configuration exception.
     *
     * @param message configuration message
     */
    public ConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates a configuration exception that is caused by another exception.
     *
     * @param message configuration error message
     * @param cause root cause of the configuration error
     */
    public ConfigurationException(String message, Exception cause) {
        super(message, cause);
    }

}
