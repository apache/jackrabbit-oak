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
package org.apache.jackrabbit.oak.security.authorization.permission;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.SubtreeValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Validator implementation that asserts that the permission store is read-only.
 */
public class PermissionStoreValidatorProvider extends ValidatorProvider implements PermissionConstants {

    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        return new SubtreeValidator(new PermissionStoreValidator(), PERMISSIONS_STORE_PATH);
    }

    private final static class PermissionStoreValidator implements Validator {

        private static final String errorMsg = "Attempt to modify permission store.";

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            throw new CommitFailedException(errorMsg);
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            throw new CommitFailedException(errorMsg);
        }

        @Override
        public void propertyDeleted(PropertyState before) throws CommitFailedException {
            throw new CommitFailedException(errorMsg);
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            throw new CommitFailedException(errorMsg);
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            throw new CommitFailedException(errorMsg);
        }

        @Override
        public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            throw new CommitFailedException(errorMsg);
        }
    }
}
