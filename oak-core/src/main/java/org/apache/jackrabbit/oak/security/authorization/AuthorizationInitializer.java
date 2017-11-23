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

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.security.authorization.permission.MountPermissionProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;

/**
 * Implementation of the {@code WorkspaceInitializer} interface that
 * <ul>
 *     <li>creates a property index definitions for
 *     {@link #REP_PRINCIPAL_NAME rep:principalName} properties defined with ACE
 *     nodes</li>
 *     <li>asserts that the permission store is setup and has dedicated entry for
 *     this workspace.</li>
 * </ul>.
 */
class AuthorizationInitializer implements WorkspaceInitializer, AccessControlConstants, PermissionConstants {

    private final MountInfoProvider mountInfoProvider;

    public AuthorizationInitializer(MountInfoProvider mountInfoProvider) {
            this.mountInfoProvider = mountInfoProvider;
    }

    @Override
    public void initialize(NodeBuilder builder, String workspaceName) {
        // property index for rep:principalName stored in ACEs
        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);
        if (!index.hasChildNode("acPrincipalName")) {
            NodeBuilder acPrincipalName = IndexUtils.createIndexDefinition(index, "acPrincipalName", true, false,
                    ImmutableList.<String>of(REP_PRINCIPAL_NAME),
                    ImmutableList.<String>of(NT_REP_DENY_ACE, NT_REP_GRANT_ACE, NT_REP_ACE));
            acPrincipalName.setProperty("info",
                    "Oak index used by authorization to quickly search a principal by name."
                    );
        }

        // create the permission store and the root for this workspace.
        NodeBuilder permissionStore =
                builder.child(JCR_SYSTEM).child(REP_PERMISSION_STORE);
        if (!permissionStore.hasProperty(JCR_PRIMARYTYPE)) {
            permissionStore.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
        }
        if (!permissionStore.hasChildNode(workspaceName)) {
            permissionStore.child(workspaceName).setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
        }
        for (Mount m : mountInfoProvider.getNonDefaultMounts()) {
            String permissionRootName =  MountPermissionProvider.getPermissionRootName(m, workspaceName);
            if (!permissionStore.hasChildNode(permissionRootName)) {
                permissionStore.child(permissionRootName).setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
            }
        }
    }

}
