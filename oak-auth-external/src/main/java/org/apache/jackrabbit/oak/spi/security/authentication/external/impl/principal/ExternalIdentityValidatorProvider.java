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

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code ValidatorProvider} used to assure that the system maintained properties
 * associated with external identities are only written by system sessions and
 * are consistent.
 *
 * @since Oak 1.5.3
 */
class ExternalIdentityValidatorProvider extends ValidatorProvider implements ExternalIdentityConstants {

    private final boolean isSystem;

    ExternalIdentityValidatorProvider(@Nonnull Set<Principal> principals) {
        isSystem = principals.contains(SystemPrincipal.INSTANCE);

    }

    private void checkAddModifyProperties(@Nonnull NodeState parent, @Nonnull String name, @Nonnull PropertyState propertyState) throws CommitFailedException {
        if (REP_EXTERNAL_PRINCIPAL_NAMES.equals(name)) {
            if (!isSystem) {
                throw new CommitFailedException(CommitFailedException.CONSTRAINT, 70, "Attempt to create, modify or remove the system property " + name);
            }
            Type<?> type = propertyState.getType();
            if (!Type.STRINGS.equals(type) || !propertyState.isArray()) {
                throw new CommitFailedException(CommitFailedException.CONSTRAINT, 71, "Property rep:externalPrincipalNames must be multi-valued of type STRING.");
            }
            if (!parent.hasProperty(REP_EXTERNAL_ID)) {
                throw new CommitFailedException(CommitFailedException.CONSTRAINT, 72, "Property rep:externalPrincipalNames requires rep:externalId to be present on the Node.");
            }
        }
    }

    private void checkRemoveProperties(@Nonnull NodeState parent, @Nonnull String name) throws CommitFailedException {
        if (RESERVED_PROPERTY_NAMES.contains(name)) {
            if (REP_EXTERNAL_ID.equals(name) && parent.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES)) {
                throw new CommitFailedException(CommitFailedException.CONSTRAINT, 73, "Property rep:externalId cannot be removed as long as rep:externalPrincipalNames is present.");
            }
            if (REP_EXTERNAL_PRINCIPAL_NAMES.equals(name) && !isSystem) {
                throw new CommitFailedException(CommitFailedException.CONSTRAINT, 70, "Attempt to create, modify or remove the system property " + name);
            }
        }
    }

    @Override
    protected Validator getRootValidator(@Nonnull NodeState before, @Nonnull NodeState after,
                                         @Nonnull CommitInfo info) {
        return new ExternalIdentityValidator(after);
    }

    private final class ExternalIdentityValidator extends DefaultValidator {

        private final NodeState parent;

        private ExternalIdentityValidator(@Nonnull NodeState parent) {
            this.parent = parent;
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            checkAddModifyProperties(parent, after.getName(), after);
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            checkAddModifyProperties(parent, before.getName(), after);
        }

        @Override
        public void propertyDeleted(PropertyState before) throws CommitFailedException {
            checkRemoveProperties(parent, before.getName());
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            return new ExternalIdentityValidator(after);
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            return new ExternalIdentityValidator(after);
        }

        @Override
        public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            // removal of the parent node containing a reserved property must be possible
            return null;
        }
    }
}