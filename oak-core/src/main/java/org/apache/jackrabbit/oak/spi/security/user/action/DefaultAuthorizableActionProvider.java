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
package org.apache.jackrabbit.oak.spi.security.user.action;

import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

/**
 * DefaultAuthorizableActionProvider... TODO
 */
public class DefaultAuthorizableActionProvider implements AuthorizableActionProvider {

    private final SecurityProvider securityProvider;
    private final ConfigurationParameters config;

    public DefaultAuthorizableActionProvider(SecurityProvider securityProvider,
                                             ConfigurationParameters config) {
        this.securityProvider = securityProvider;
        this.config = config;
    }

    @Override
    public List<AuthorizableAction> getAuthorizableActions() {
        // TODO OAK-521: create and initialize actions from configuration
        return Collections.emptyList();
    }
}