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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Test;

import javax.jcr.Value;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;

import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.MIX_REP_PRINCIPAL_BASED_MIXIN;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;

public class CheckedInNodeTest extends AbstractPrincipalBasedTest {

    private Principal testPrincipal;

    @Override
    public void before() throws Exception {
        super.before();

        User u = getTestSystemUser();
        testPrincipal = u.getPrincipal();

        setupContentTrees(TEST_OAK_PATH);

        Tree userTree = root.getTree(getNamePathMapper().getOakPath(u.getPath()));
        TreeUtil.addMixin(userTree, MIX_REP_PRINCIPAL_BASED_MIXIN, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        TreeUtil.addMixin(userTree, MIX_VERSIONABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        root.commit();

        userTree.setProperty(JCR_ISCHECKEDOUT, false);
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            Tree userTree = root.getTree(getNamePathMapper().getOakPath(getTestSystemUser().getPath()));
            userTree.setProperty(JCR_ISCHECKEDOUT, true);
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testAddEmptyPolicy() throws Exception {
        JackrabbitAccessControlManager acMgr = getAccessControlManager(root);
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, acMgr);
        acMgr.setPolicy(policy.getPath(), policy);
        root.commit();
    }

    @Test
    public void testAddEntry() throws Exception {
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(testPrincipal, getNamePathMapper().getJcrPath(TEST_OAK_PATH), JCR_READ);
        root.commit();
    }

    @Test
    public void testAddEntryWithRestrictions() throws Exception {
        JackrabbitAccessControlManager acMgr = getAccessControlManager(root);
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, acMgr);
        Map<String, Value> restrictions = Collections.singletonMap(getNamePathMapper().getJcrName(REP_GLOB), getValueFactory(root).createValue("/*"));
        policy.addEntry(getNamePathMapper().getJcrPath(TEST_OAK_PATH), privilegesFromNames(JCR_READ), restrictions, Collections.emptyMap());
        acMgr.setPolicy(policy.getPath(), policy);
        root.commit();
    }
}