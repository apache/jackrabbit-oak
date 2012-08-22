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
package org.apache.jackrabbit.oak.core;

import java.security.Principal;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlContext;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy implementation that omits any permission checks
 */
public class TestAcContext implements AccessControlContext {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(TestAcContext.class);

    @Override
    public void initialize(Set<Principal> principals) {
        // nop

    }

    @Override
    public CompiledPermissions getPermissions() {
        return new CompiledPermissions() {
            @Override
            public boolean canRead(String path, boolean isProperty) {
                return true;
            }

            @Override
            public boolean isGranted(int permissions) {
                return true;
            }

            @Override
            public boolean isGranted(Tree tree, int permissions) {
                return true;
            }

            @Override
            public boolean isGranted(Tree parent, PropertyState property, int permissions) {
                return true;
            }
        };
    }

    @Override
    public ValidatorProvider getPermissionValidatorProvider(CoreValueFactory valueFactory) {
        return new DefaultValidatorProvider();
    }

    @Override
    public ValidatorProvider getAccessControlValidatorProvider(CoreValueFactory valueFactory) {
        return new DefaultValidatorProvider();
    }
}
