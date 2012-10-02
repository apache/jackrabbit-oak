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

import java.util.Collections;
import java.util.List;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;

/**
 * This class implements an {@link AccessControlProvider} which grants
 * full access to any {@link Subject} passed to {@link #createAccessControlContext(Subject)}.
 */
public class OpenAccessControlProvider
        implements AccessControlProvider {

    @Override
    public AccessControlContext createAccessControlContext(Subject subject) {
        return new AccessControlContext() {
            @Override
            public CompiledPermissions getPermissions() {
                return AllPermissions.INSTANCE;
            }
        };
    }

    @Override
    public List<ValidatorProvider> getValidatorProviders() {
        return Collections.emptyList();
    }

    private static final class AllPermissions implements CompiledPermissions {

        private static final CompiledPermissions INSTANCE = new AllPermissions();

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
        public boolean isGranted(Tree parent,
                                 PropertyState property,
                                 int permissions) {
            return true;
        }
    }
}
