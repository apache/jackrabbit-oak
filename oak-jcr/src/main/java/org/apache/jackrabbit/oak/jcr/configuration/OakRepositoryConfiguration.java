/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.jcr.configuration;

import javax.jcr.RepositoryException;
import java.util.Collections;
import java.util.Map;

import static java.text.MessageFormat.format;

public class OakRepositoryConfiguration implements RepositoryConfiguration {
    private final Map<String, String> parameters;
    private final String microkernelUrl;
    private final int nodeStateCacheSize;

    private OakRepositoryConfiguration(Map<String, String> parameters) throws RepositoryException {
        this.parameters = Collections.unmodifiableMap(parameters);

        microkernelUrl = getParameterMap().get(MICROKERNEL_URL);
        if (microkernelUrl == null) {
            throw new RepositoryException(format("Missing configuration value for {0}", MICROKERNEL_URL));
        }

        String size = getParameterMap().get(NODE_STATE_CACHE_SIZE);
        try {
            nodeStateCacheSize = size == null ? 0 : Integer.parseInt(size);
        }
        catch (NumberFormatException e) {
            throw new RepositoryException(format("Invalid configuration value {0} for {1}",
                    size, NODE_STATE_CACHE_SIZE), e);
        }

    }

    public static RepositoryConfiguration create(Map<String, String> parameters) throws RepositoryException {
        return new OakRepositoryConfiguration(parameters);
    }

    @Override
    public final Map<String, String> getParameterMap() {
        return parameters;
    }

    @Override
    public String getMicrokernelUrl() throws RepositoryException {
        return microkernelUrl;
    }

    @Override
    public int getNodeStateCacheSize() {
        return nodeStateCacheSize;
    }
}
