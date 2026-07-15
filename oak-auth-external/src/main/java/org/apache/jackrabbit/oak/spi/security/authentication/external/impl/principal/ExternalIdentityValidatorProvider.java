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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * {@code ValidatorProvider} used to assure that the system maintained properties
 * associated with external identities are only written by system sessions and
 * are consistent.
 *
 * @since Oak 1.5.3
 */
class ExternalIdentityValidatorProvider extends ValidatorProvider implements ExternalIdentityConstants {

    private static final Map<Integer, String> ERROR_MSGS = Map.of(
            70, "Attempt to create, modify or remove the system property 'rep:externalPrincipalNames'",
            71, "Property 'rep:externalPrincipalNames' must be multi-valued of type STRING.",
            72, "Property 'rep:externalPrincipalNames' requires 'rep:externalId' to be present on the Node.",
            73, "Property 'rep:externalId' cannot be removed as long as 'rep:externalPrincipalNames' is present.",
            74, "Attempt to add, modify or remove the system maintained property 'rep:externalId'.",
            75, "Property 'rep:externalId' may only have a single value of type STRING.");

    private final boolean isSystem;
    private final boolean protectedExternalIds;

    ExternalIdentityValidatorProvider(boolean isSystem, boolean protectExternalIds) {
        this.isSystem = isSystem;
        this.protectedExternalIds = protectExternalIds;

    }

    @Override
    protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        return new ExternalIdentityValidator(after, true);
    }

    private final class ExternalIdentityValidator extends DefaultValidator {

        private final NodeState parent;
        private final boolean modifiedParent;

        private ExternalIdentityValidator(@NotNull NodeState parent, boolean modifiedParent) {
            this.parent = parent;
            this.modifiedParent = modifiedParent;
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            checkAddModifyProperties(parent, after.getName(), after, modifiedParent);
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            checkAddModifyProperties(parent, before.getName(), after, modifiedParent);
        }

        private void checkAddModifyProperties(@NotNull NodeState parent, @NotNull String name, @NotNull PropertyState propertyState, boolean isModify) throws CommitFailedException {
            if (!RESERVED_PROPERTY_NAMES.contains(name)) {
                return;
            }
            if (REP_EXTERNAL_PRINCIPAL_NAMES.equals(name)) {
                checkIsSystem();
                if (!Type.STRINGS.equals(propertyState.getType()) || !propertyState.isArray()) {
                    throw new CommitFailedException(CommitFailedException.CONSTRAINT, 71, ERROR_MSGS.get(71));
                }
                if (!parent.hasProperty(REP_EXTERNAL_ID)) {
                    throw new CommitFailedException(CommitFailedException.CONSTRAINT, 72, ERROR_MSGS.get(72));
                }
            }
            if (REP_EXTERNAL_ID.equals(name) && protectedExternalIds) {
                if (isModify && !isSystem) {
                    throw new CommitFailedException(CommitFailedException.CONSTRAINT, 74, ERROR_MSGS.get(74));
                }
                if (!Type.STRING.equals(propertyState.getType()) || propertyState.isArray()) {
                    throw new CommitFailedException(CommitFailedException.CONSTRAINT, 75, ERROR_MSGS.get(75));
                }
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) throws CommitFailedException {
            String name = before.getName();
            if (!RESERVED_PROPERTY_NAMES.contains(name)) {
                return;
            }
            if (REP_EXTERNAL_PRINCIPAL_NAMES.equals(name)) {
                checkIsSystem();
            } 
            if (REP_EXTERNAL_ID.equals(name)) {
                if (parent.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES)) {
                    throw new CommitFailedException(CommitFailedException.CONSTRAINT, 73, ERROR_MSGS.get(73));
                }
                if (protectedExternalIds && !isSystem) {
                    throw new CommitFailedException(CommitFailedException.CONSTRAINT, 74, ERROR_MSGS.get(74));
                }
            }
        }
        
        private void checkIsSystem() throws CommitFailedException {
            if (!isSystem) {
                throw new CommitFailedException(CommitFailedException.CONSTRAINT, 70, ERROR_MSGS.get(70));
            }
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) {
            return new ExternalIdentityValidator(after, false);
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) {
            return new ExternalIdentityValidator(after, true);
        }

        @Override
        public @Nullable Validator childNodeDeleted(String name, NodeState before) {
            // removal of the parent node containing a reserved property must be possible
            return null;
        }
    }
}
