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

import java.util.List;

import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AuthorizationContextTest extends AbstractSecurityTest {

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2740">OAK-2740</a>
     */
    @Test
    public void testItemDefinitionsDefinesContextRoot() throws Exception {
        List<String> paths = Lists.newArrayList(
                "/jcr:system/jcr:nodeTypes/rep:AccessControllable/rep:namedChildNodeDefinitions/rep:policy",
                "/jcr:system/jcr:nodeTypes/rep:RepoAccessControllable/rep:namedChildNodeDefinitions/rep:repoPolicy");

        for (String defPath : paths) {
            Tree tree = root.getTree(defPath);
            assertFalse(AuthorizationContext.getInstance().definesContextRoot(tree));
        }
    }

    @Test
    public void testPolicyDefinesContextRoot() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);

        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
        assertNotNull(acl);

        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy("/", acl);

        Tree aclTree = root.getTree("/").getChild(AccessControlConstants.REP_POLICY);
        assertTrue(aclTree.exists());
        assertTrue(AuthorizationContext.getInstance().definesContextRoot(aclTree));

        // revert changes
        root.refresh();
    }

    @Test
    public void testRepoPolicyDefinesContextRoot() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);

        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, null);
        assertNotNull(acl);

        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT));
        acMgr.setPolicy(null, acl);

        Tree aclTree = root.getTree("/").getChild(AccessControlConstants.REP_REPO_POLICY);
        assertTrue(aclTree.exists());
        assertTrue(AuthorizationContext.getInstance().definesContextRoot(aclTree));

        // revert changes
        root.refresh();
    }

    @Test
    public void testAceDefinesContextRoot() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);

        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
        assertNotNull(acl);

        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy("/", acl);

        Tree aclTree = root.getTree("/").getChild(AccessControlConstants.REP_POLICY);
        assertTrue(aclTree.exists());

        for (Tree child : aclTree.getChildren()) {
            assertFalse(AuthorizationContext.getInstance().definesContextRoot(child));
        }

        // revert changes
        root.refresh();
    }

}