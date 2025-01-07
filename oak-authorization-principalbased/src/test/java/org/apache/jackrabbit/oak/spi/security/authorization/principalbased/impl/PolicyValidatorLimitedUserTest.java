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

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.SimpleCredentials;

import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.MIX_REP_PRINCIPAL_BASED_MIXIN;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_ENTRY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_NAME;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PolicyValidatorLimitedUserTest extends AbstractPrincipalBasedTest {

    private String accessControlledPath;
    private ContentSession testSession;
    private Root testRoot;

    @Before
    public void before() throws Exception {
        super.before();

        User systemUser = getTestSystemUser();
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(systemUser.getPrincipal(), testJcrPath, JCR_NODE_TYPE_MANAGEMENT);
        accessControlledPath = policy.getOakPath();

        User testUser = getTestUser();
        addDefaultEntry(PathUtils.ROOT_PATH, testUser.getPrincipal(), JCR_READ, JCR_READ_ACCESS_CONTROL);
        root.commit();

        testSession = login(new SimpleCredentials(testUser.getID(), testUser.getID().toCharArray()));
        testRoot = testSession.getLatestRoot();
    }

    @After
    public void after() throws Exception {
        try {
            testRoot.refresh();
            if (testSession != null) {
                testSession.close();
            }
        } finally {
            super.after();
        }
    }

    @NotNull
    private Tree createPolicyEntryTree(@NotNull Root r, @NotNull String effectiveOakPath, @NotNull String... privNames) throws Exception {
        Tree t = r.getTree(accessControlledPath);
        TreeUtil.addMixin(t, MIX_REP_PRINCIPAL_BASED_MIXIN, r.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");
        Tree policy = TreeUtil.addChild(t, REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY);
        policy.setProperty(REP_PRINCIPAL_NAME, getTestSystemUser().getPrincipal().getName());
        Tree entry = TreeUtil.addChild(policy, "entry", NT_REP_PRINCIPAL_ENTRY);
        entry.setProperty(REP_EFFECTIVE_PATH, effectiveOakPath, Type.PATH);
        entry.setProperty(REP_PRIVILEGES, Set.of(privNames), Type.NAMES);
        return entry;
    }

    @Test
    public void testAddEntryMissingModAcPermission() throws Exception {
        Tree entry = createPolicyEntryTree(testRoot, TEST_OAK_PATH, JCR_READ);
        try {
            testRoot.commit();
            fail("CommitFailedException expected; type ACCESS; code 3");
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
        }
    }

    @Test
    public void testChangeEntryMissingModAcPermission() throws Exception {
        Tree entry = createPolicyEntryTree(root, TEST_OAK_PATH, JCR_READ);
        root.commit();
        testRoot.refresh();

        entry = testRoot.getTree(entry.getPath());
        entry.setProperty(REP_PRIVILEGES, Set.of(JCR_READ, JCR_WRITE), Type.NAMES);
        try {
            testRoot.commit();
            fail("CommitFailedException expected; type ACCESS; code 3");
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
        }
    }

    @Test
    public void testAddRestrictionMissingModAcPermission() throws Exception {
        Tree entry = createPolicyEntryTree(root, TEST_OAK_PATH, JCR_READ);
        root.commit();
        testRoot.refresh();

        entry = testRoot.getTree(entry.getPath());
        Tree restrictions = TreeUtil.addChild(entry, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setProperty(REP_GLOB, "*/glob/*");
        try {
            testRoot.commit();
            fail("CommitFailedException expected; type ACCESS; code 3");
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
        }
    }

    @Test
    public void testModifyRestrictionMissingModAcPermission() throws Exception {
        Tree entry = createPolicyEntryTree(root, TEST_OAK_PATH, JCR_READ);
        Tree restrictions = TreeUtil.addChild(entry, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setProperty(REP_GLOB, "*/glob/*");
        root.commit();
        testRoot.refresh();

        restrictions = testRoot.getTree(restrictions.getPath());
        restrictions.setProperty(REP_GLOB, "*/changedGlob/*");
        try {
            testRoot.commit();
            fail("CommitFailedException expected; type ACCESS; code 3");
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
        }
    }

}