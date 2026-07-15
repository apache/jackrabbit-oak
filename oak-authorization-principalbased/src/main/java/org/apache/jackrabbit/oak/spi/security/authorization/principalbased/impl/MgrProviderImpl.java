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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.jetbrains.annotations.NotNull;

final class MgrProviderImpl implements MgrProvider {

    private final PrincipalBasedAuthorizationConfiguration config;

    private NamePathMapper namePathMapper;
    private Root root;
    private Context ctx;
    private RestrictionProvider restrictionProvider;
    private PrincipalManager principalManager;
    private PrivilegeManager privilegeManager;
    private PrivilegeBitsProvider privilegeBitsProvider;

    MgrProviderImpl(@NotNull PrincipalBasedAuthorizationConfiguration config) {
        this.config = config;
        this.namePathMapper = NamePathMapper.DEFAULT;
    }

    MgrProviderImpl(@NotNull PrincipalBasedAuthorizationConfiguration config, @NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        this.config = config;
        reset(root, namePathMapper);
    }

    @NotNull
    @Override
    public SecurityProvider getSecurityProvider() {
        return config.getSecurityProvider();
    }

    @Override
    public void reset(@NotNull Root root, NamePathMapper namePathMapper) {
        this.root = root;
        this.namePathMapper = namePathMapper;

        this.ctx = null;
        this.restrictionProvider = null;
        this.principalManager = null;
        this.privilegeManager = null;
        this.privilegeBitsProvider = null;
    }

    @NotNull
    @Override
    public Root getRoot() {
        checkRootInitialized();
        return root;
    }

    @NotNull
    @Override
    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    @NotNull
    @Override
    public Context getContext() {
        if (ctx == null) {
            // make sure the context allows to reveal any kind of protected access control/permission content not just
            // those defined by this module.
            ctx = getSecurityProvider().getConfiguration(AuthorizationConfiguration.class).getContext();
        }
        return ctx;
    }

    @NotNull
    @Override
    public PrivilegeManager getPrivilegeManager() {
        checkRootInitialized();
        if (privilegeManager == null) {
            privilegeManager = getSecurityProvider().getConfiguration(PrivilegeConfiguration.class).getPrivilegeManager(root, namePathMapper);
        }
        return privilegeManager;
    }

    @NotNull
    @Override
    public PrivilegeBitsProvider getPrivilegeBitsProvider() {
        checkRootInitialized();
        if (privilegeBitsProvider == null) {
            privilegeBitsProvider = new PrivilegeBitsProvider(root);
        }
        return privilegeBitsProvider;
    }

    @NotNull
    @Override
    public PrincipalManager getPrincipalManager() {
        checkRootInitialized();
        if (principalManager == null) {
            principalManager = getSecurityProvider().getConfiguration(PrincipalConfiguration.class).getPrincipalManager(root, namePathMapper);
        }
        return principalManager;
    }

    @NotNull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        if (restrictionProvider == null) {
            restrictionProvider = getSecurityProvider().getConfiguration(AuthorizationConfiguration.class).getRestrictionProvider();
        }
        return restrictionProvider;
    }

    @NotNull
    @Override
    public TreeProvider getTreeProvider() {
        return config.getTreeProvider();
    }

    @NotNull
    @Override
    public RootProvider getRootProvider() {
        return config.getRootProvider();
    }

    private void checkRootInitialized() {
        Validate.checkState(root != null);
    }
}