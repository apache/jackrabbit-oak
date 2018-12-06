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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CUG specific implementation of the {@code ProtectedPropertyImporter}.
 */
class CugImporter implements ProtectedPropertyImporter, CugConstants {

    private static final Logger log = LoggerFactory.getLogger(CugImporter.class);

    private final MountInfoProvider mountInfoProvider;

    private boolean initialized;

    private Set<String> supportedPaths;
    private int importBehavior;

    private PrincipalManager principalManager;

    CugImporter(@NotNull MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
    }

    //----------------------------------------------< ProtectedItemImporter >---
    @Override
    public boolean init(@NotNull Session session, @NotNull Root root, @NotNull NamePathMapper namePathMapper, boolean isWorkspaceImport, int uuidBehavior, @NotNull ReferenceChangeTracker referenceTracker, @NotNull SecurityProvider securityProvider) {
        if (initialized) {
            throw new IllegalStateException("Already initialized");
        }
        try {
            ConfigurationParameters config = securityProvider.getConfiguration(AuthorizationConfiguration.class).getParameters();
            supportedPaths = CugUtil.getSupportedPaths(config, mountInfoProvider);
            importBehavior = CugUtil.getImportBehavior(config);

            if (isWorkspaceImport) {
                PrincipalConfiguration pConfig = securityProvider.getConfiguration(PrincipalConfiguration.class);
                principalManager = pConfig.getPrincipalManager(root, namePathMapper);
            } else {
                principalManager = ((JackrabbitSession) session).getPrincipalManager();
            }
            initialized = true;
        } catch (RepositoryException e) {
            log.warn("Error while initializing cug importer", e);
        }
        return initialized;
    }

    @Override
    public void processReferences() {
        // nothing to do
    }

    //------------------------------------------< ProtectedPropertyImporter >---

    @Override
    public boolean handlePropInfo(@NotNull Tree parent, @NotNull PropInfo protectedPropInfo, @NotNull PropertyDefinition def) {
        if (CugUtil.definesCug(parent) && isValid(protectedPropInfo, def) && CugUtil.isSupportedPath(parent.getPath(), supportedPaths)) {
            Set<String> principalNames = new HashSet<>();
            for (TextValue txtValue : protectedPropInfo.getTextValues()) {
                String principalName = txtValue.getString();
                Principal principal = principalManager.getPrincipal(principalName);
                if (principal == null) {
                    switch (importBehavior) {
                        case ImportBehavior.IGNORE:
                            log.debug("Ignoring unknown principal with name '" + principalName + "'.");
                            break;
                        case ImportBehavior.ABORT:
                            throw new AccessControlException("Unknown principal '" + principalName + "'.");
                        case ImportBehavior.BESTEFFORT:
                            log.debug("Importing unknown principal '" + principalName + '\'');
                            principalNames.add(principalName);
                            break;
                        default:
                            throw new IllegalArgumentException("Invalid import behavior " + importBehavior);
                    }
                } else {
                    principalNames.add(principalName);
                }
            }
            parent.setProperty(REP_PRINCIPAL_NAMES, principalNames, Type.STRINGS);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void propertiesCompleted(@NotNull Tree protectedParent) throws IllegalStateException {
        if (CugUtil.definesCug(protectedParent) && !protectedParent.hasProperty(REP_PRINCIPAL_NAMES)) {
            // remove the rep:cugPolicy node if mandatory property is missing
            // (which may also happen upon an attempt to create a cug at an unsupported path).
            log.debug("Removing incomplete rep:cugPolicy node (due to missing mandatory property or unsupported path).");
            protectedParent.remove();
        }
    }

    //--------------------------------------------------------------------------
    private boolean isValid(@NotNull PropInfo propInfo, @NotNull PropertyDefinition def) {
        if (REP_PRINCIPAL_NAMES.equals(propInfo.getName())) {
            return def.isMultiple() && NT_REP_CUG_POLICY.equals(def.getDeclaringNodeType().getName());
        }
        return false;
    }
}
