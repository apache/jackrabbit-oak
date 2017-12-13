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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code AccessControlValidatorProvider} aimed to provide a root validator
 * that makes sure access control related content modifications (adding, modifying
 * and removing access control policies) are valid according to the
 * constraints defined by this access control implementation.
 */
public class AccessControlValidatorProvider extends ValidatorProvider {

    private final SecurityProvider securityProvider;
    private final RootProvider rootProvider;
    private final TreeProvider treeProvider;

    public AccessControlValidatorProvider(@Nonnull SecurityProvider securityProvider, @Nonnull RootProvider rootProvider, @Nonnull TreeProvider treeProvider) {
        this.securityProvider = securityProvider;
        this.rootProvider = rootProvider;
        this.treeProvider = treeProvider;
    }

    //--------------------------------------------------< ValidatorProvider >---
    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {

        RestrictionProvider restrictionProvider = getConfig(AuthorizationConfiguration.class).getRestrictionProvider();

        Root root = rootProvider.createReadOnlyRoot(before);
        PrivilegeManager privilegeManager = getConfig(PrivilegeConfiguration.class).getPrivilegeManager(root, NamePathMapper.DEFAULT);
        PrivilegeBitsProvider privilegeBitsProvider = new PrivilegeBitsProvider(root);

        return new AccessControlValidator(after, privilegeManager, privilegeBitsProvider, restrictionProvider, treeProvider);
    }

    private <T> T getConfig(Class<T> configClass) {
        return securityProvider.getConfiguration(configClass);
    }
}
