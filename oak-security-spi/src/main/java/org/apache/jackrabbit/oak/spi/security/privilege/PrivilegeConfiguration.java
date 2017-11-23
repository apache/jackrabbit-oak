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
package org.apache.jackrabbit.oak.spi.security.privilege;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;

/**
 * Interface for the privilege management configuration.
 */
public interface PrivilegeConfiguration extends SecurityConfiguration {

    String NAME = "org.apache.jackrabbit.oak.privilege";

    /**
     * Creates a new instance of {@link PrivilegeManager}.
     *
     * @param root           The root for which the privilege manager should be created.
     * @param namePathMapper The name and path mapper to be used.
     * @return A new {@code PrivilegeManager}.
     */
    @Nonnull
    PrivilegeManager getPrivilegeManager(Root root, NamePathMapper namePathMapper);
}
