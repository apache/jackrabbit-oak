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

import java.util.Set;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.PRIVILEGES_PATH;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_AGGREGATES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class JcrAllCommitHookTest extends AbstractSecurityTest {

    private PrivilegeManager privilegeManager;
    private Privilege newPrivilege;

    private final JcrAllCommitHook hook = new JcrAllCommitHook();

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        privilegeManager = getPrivilegeManager(root);
        newPrivilege = privilegeManager.registerPrivilege("abstractPrivilege", true, null);
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @Test
    public void testJcrAll() throws Exception {
        Privilege all = privilegeManager.getPrivilege(JCR_ALL);
        Set<Privilege> aggregates = Sets.newHashSet(all.getDeclaredAggregatePrivileges());

        assertTrue(aggregates.contains(newPrivilege));
    }

    @Test
    public void testToString() {
        assertEquals("JcrAllCommitHook", hook.toString());
    }

    @Test
    public void testJcrAllNodeAdded() {
        root.getTree(PRIVILEGES_PATH).getChild(JCR_ALL).remove();
        NodeState before = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));

        Root r = adminSession.getLatestRoot();
        NodeState after = getTreeProvider().asNodeState(r.getTree(PathUtils.ROOT_PATH));
        hook.processCommit(before, after, null);
    }

    @Test
    public void testJcrAllNodeWithoutAggregates() {
        NodeState before = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));

        Root r = adminSession.getLatestRoot();
        Tree t = r.getTree(PRIVILEGES_PATH);
        Tree jcrAll = t.getChild(JCR_ALL);
        jcrAll.removeProperty(REP_AGGREGATES);
        t.addChild("newPriv");
        NodeState after = getTreeProvider().asNodeState(r.getTree(PathUtils.ROOT_PATH));
        hook.processCommit(before, after, null);
    }

    @Test
    public void testJcrAllNodeAlreadyContainsNewName() {
        NodeState before = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));

        Root r = adminSession.getLatestRoot();
        Tree t = r.getTree(PRIVILEGES_PATH);
        Tree jcrAll = t.getChild(JCR_ALL);
        jcrAll.setProperty(REP_AGGREGATES, ImmutableList.of("newPriv"), Type.NAMES);
        t.addChild("newPriv");
        NodeState after = getTreeProvider().asNodeState(r.getTree(PathUtils.ROOT_PATH));
        hook.processCommit(before, after, null);
    }

    @Test
    public void testPrivilegesRootAdded() {
        root.getTree(PRIVILEGES_PATH).remove();
        NodeState before = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));

        Root r = adminSession.getLatestRoot();
        NodeState after = getTreeProvider().asNodeState(r.getTree(PathUtils.ROOT_PATH));
        hook.processCommit(before, after, null);
    }
}