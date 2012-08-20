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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlContext;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;

/**
 * PermissionValidatorProvider... TODO
 */
class PermissionValidatorProvider implements ValidatorProvider {

    private final CoreValueFactory coreValueFactory;
    private final AccessControlContext accessControlContext;

    public PermissionValidatorProvider(CoreValueFactory coreValueFactory, AccessControlContext accessControlContext) {
        this.coreValueFactory = coreValueFactory;
        this.accessControlContext = accessControlContext;
    }

    //--------------------------------------------------< ValidatorProvider >---
    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        NodeUtil rootBefore = new NodeUtil(new ReadOnlyTree(before), coreValueFactory, NamePathMapper.DEFAULT);
        NodeUtil rootAfter = new NodeUtil(new ReadOnlyTree(after), coreValueFactory, NamePathMapper.DEFAULT);
        return new PermissionValidator(accessControlContext.getPermissions(), rootBefore, rootAfter);
    }
}
