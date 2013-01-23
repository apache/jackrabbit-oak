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
package org.apache.jackrabbit.oak.spi.security.authorization;

import javax.jcr.NamespaceRegistry;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.security.authorization.AccessControlManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * AbstractAccessControlTest... TODO
 */
public abstract class AbstractAccessControlTest extends AbstractSecurityTest {

    private PrivilegeManager privMgr;
    private RestrictionProvider restrictionProvider;

    protected void registerNamespace(String prefix, String uri) throws Exception {
        NamespaceRegistry nsRegistry = new ReadWriteNamespaceRegistry() {
            @Override
            protected Root getWriteRoot() {
                return root;
            }

            @Override
            protected Tree getReadTree() {
                return root.getTree("/");
            }
        };
        nsRegistry.registerNamespace(prefix, uri);
    }

    protected NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    protected Privilege[] privilegesFromNames(String... privilegeNames) throws Exception {
        Privilege[] privs = new Privilege[privilegeNames.length];
        for (int i = 0; i < privilegeNames.length; i++) {
            privs[i] = getPrivilegeManager().getPrivilege(privilegeNames[i]);
        }
        return privs;
    }

    protected JackrabbitAccessControlManager getAccessControlManager(Root root) {
        // TODO
        //acMgr = securityProvider.getAccessControlConfiguration().getAccessControlManager(root, NamePathMapper.DEFAULT);
        return new AccessControlManagerImpl(root, getNamePathMapper(), getSecurityProvider());
    }

    protected RestrictionProvider getRestrictionProvider() {
        if (restrictionProvider == null) {
            restrictionProvider = getSecurityProvider().getAccessControlConfiguration().getRestrictionProvider(getNamePathMapper());
        }
        return restrictionProvider;
    }

    protected PrivilegeManager getPrivilegeManager() {
        if (privMgr == null) {
            privMgr = getSecurityProvider().getPrivilegeConfiguration().getPrivilegeManager(root, getNamePathMapper());
        }
        return privMgr;
    }
}