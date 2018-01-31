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
package org.apache.jackrabbit.oak.security.privilege;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.SubtreeValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_PRIVILEGES;

/**
 * {@code PrivilegeValidatorProvider} to construct a {@code Validator} instance
 * to make sure modifications to the /jcr:system/rep:privileges tree are compliant
 * with constraints applied for custom privileges.
 */
class PrivilegeValidatorProvider extends ValidatorProvider {

    private final RootProvider rootProvider;

    PrivilegeValidatorProvider(@Nonnull RootProvider rootProvider) {
        this.rootProvider = rootProvider;
    }

    @Nonnull
    @Override
    public Validator getRootValidator(
            NodeState before, NodeState after, CommitInfo info) {
        return new SubtreeValidator(new PrivilegeValidator(createRoot(before), createRoot(after)),
                JCR_SYSTEM, REP_PRIVILEGES);
    }

    private Root createRoot(NodeState nodeState) {
        return rootProvider.createReadOnlyRoot(nodeState);
    }
}