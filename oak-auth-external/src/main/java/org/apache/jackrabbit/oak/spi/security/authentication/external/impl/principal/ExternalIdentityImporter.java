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

import javax.annotation.Nonnull;
import javax.jcr.Session;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link ProtectedPropertyImporter} interface to properly
 * handle reserved system maintained properties defined by this module.
 *
 * @since Oak 1.5.3
 */
class ExternalIdentityImporter implements ProtectedPropertyImporter, ExternalIdentityConstants {

    private static final Logger log = LoggerFactory.getLogger(ExternalIdentityImporter.class);

    private boolean isSystemSession;

    //----------------------------------------------< ProtectedItemImporter >---
    @Override
    public boolean init(@Nonnull Session session, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper, boolean isWorkspaceImport, int uuidBehavior, @Nonnull ReferenceChangeTracker referenceTracker, @Nonnull SecurityProvider securityProvider) {
        isSystemSession = root.getContentSession().getAuthInfo().getPrincipals().contains(SystemPrincipal.INSTANCE);
        return true;
    }

    @Override
    public void processReferences() {
        // nothing to
    }

    //------------------------------------------< ProtectedPropertyImporter >---
    /**
     * Due to the fact that the reserved external-identity properties are
     * not protected from a JCR (item definition) point of view, the handling
     * of the system maintained properties needs to be postpone to the {@link #propertiesCompleted} call.
     *
     * @param parent The affected parent node.
     * @param protectedPropInfo The {@code PropInfo} to be imported.
     * @param def The property definition determined by the importer that
     * calls this method.
     * @return Always returns false.
     */
    @Override
    public boolean handlePropInfo(@Nonnull Tree parent, @Nonnull PropInfo protectedPropInfo, @Nonnull PropertyDefinition def) {
        return false;
    }

    /**
     * Prevent 'rep:externalPrincipalNames' properties from being imported with a
     * non-system session.
     * Note: in order to make sure those properties are synchronized again upon
     * the next login, 'rep:lastSynced' property gets removed as well.
     *
     * @param protectedParent The protected parent tree.
     */
    @Override
    public void propertiesCompleted(@Nonnull Tree protectedParent) {
        if (!isSystemSession) {
            if (protectedParent.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES)) {
                log.debug("Found reserved property " + REP_EXTERNAL_PRINCIPAL_NAMES + " managed by the system => Removed from imported scope.");
                protectedParent.removeProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
                // force creation of rep:externalPrincipalNames by removing the
                // rep:lastSynced property as well.
                protectedParent.removeProperty(REP_LAST_SYNCED);
            }
        }
    }
}