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
package org.apache.jackrabbit.oak.security.authorization;

import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinitionReader;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code AccessControlValidatorProvider} aimed to provide a root validator
 * that makes sure access control related content modifications (adding, modifying
 * and removing access control policies) are valid according to the
 * constraints defined by this access control implementation.
 */
class AccessControlValidatorProvider implements ValidatorProvider {

    private SecurityProvider securityProvider;

    AccessControlValidatorProvider(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    //--------------------------------------------------< ValidatorProvider >---
    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        Tree rootBefore = new ReadOnlyTree(before);
        Tree rootAfter = new ReadOnlyTree(after);

        PrivilegeDefinitionReader reader = securityProvider.getPrivilegeConfiguration().getPrivilegeDefinitionReader(rootBefore);
        Map<String, PrivilegeDefinition> privilegeDefinitions = reader.readDefinitions();

        AccessControlConfiguration acConfig = securityProvider.getAccessControlConfiguration();
        RestrictionProvider restrictionProvider = acConfig.getRestrictionProvider(NamePathMapper.DEFAULT);

        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(before);

        return new AccessControlValidator(rootBefore, rootAfter, privilegeDefinitions, restrictionProvider, ntMgr);
    }

}
