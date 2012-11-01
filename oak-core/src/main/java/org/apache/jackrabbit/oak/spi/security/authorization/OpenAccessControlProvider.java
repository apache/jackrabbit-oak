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
package org.apache.jackrabbit.oak.spi.security.authorization;

import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;

/**
 * This class implements an {@link AccessControlProvider} which grants
 * full access to any {@link Subject} passed to {@link #getAccessControlContext(Subject)}.
 */
public class OpenAccessControlProvider extends SecurityConfiguration.Default
        implements AccessControlProvider {

    @Override
    public AccessControlContext getAccessControlContext(Subject subject) {
        return new AccessControlContext() {
            @Override
            public CompiledPermissions getPermissions() {
                return AllPermissions.getInstance();
            }
        };
    }
}
