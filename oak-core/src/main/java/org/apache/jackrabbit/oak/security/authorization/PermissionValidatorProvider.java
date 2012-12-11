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

import java.security.AccessController;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;

/**
 * PermissionValidatorProvider... TODO
 */
class PermissionValidatorProvider implements ValidatorProvider {

    private final SecurityProvider securityProvider;
    private final AccessControlConfiguration acConfiguration;

    PermissionValidatorProvider(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
        this.acConfiguration = securityProvider.getAccessControlConfiguration();
    }

    //--------------------------------------------------< ValidatorProvider >---
    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        Subject subject = Subject.getSubject(AccessController.getContext());
        Set<Principal> principals = (subject != null) ? subject.getPrincipals() : Collections.<Principal>emptySet();
        CompiledPermissions permissions = acConfiguration.getPermissionProvider(NamePathMapper.DEFAULT).getCompiledPermissions(/*TODO*/null, principals);

        NodeUtil rootBefore = new NodeUtil(new ReadOnlyTree(before));
        NodeUtil rootAfter = new NodeUtil(new ReadOnlyTree(after));
        return new PermissionValidator(rootBefore, rootAfter, permissions, this);
    }

    //-----------------------------------------------------------< internal >---
    Context getUserContext() {
        return securityProvider.getUserConfiguration().getContext();
    }

    Context getPrivilegeContext() {
        return securityProvider.getPrivilegeConfiguration().getContext();
    }

    Context getAccessControlContext() {
        return acConfiguration.getContext();
    }
}
