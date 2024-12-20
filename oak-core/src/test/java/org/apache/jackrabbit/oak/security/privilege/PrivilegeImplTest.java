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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PrivilegeImplTest extends AbstractSecurityTest implements PrivilegeConstants {

    private Privilege privilege;
    private Privilege abstractPrivilege;
    private Privilege allPrivilege;
    private Privilege aggrPrivilege;

    @Override
    public void before() throws Exception {
        super.before();

        PrivilegeManager pMgr = getPrivilegeManager(root);
        privilege = pMgr.getPrivilege(JCR_READ_ACCESS_CONTROL);
        aggrPrivilege = pMgr.getPrivilege(REP_WRITE);
        allPrivilege = pMgr.getPrivilege(JCR_ALL);
        abstractPrivilege = pMgr.registerPrivilege("abstractPrivilege", true, null);
    }

    @Override
    public void after() throws Exception {
        root.refresh();
        super.after();
    }

    private static void assertAggregation(@NotNull Privilege[] aggr, @NotNull String... expectedNames) {
        assertEquals(expectedNames.length, aggr.length);

        Set<String> expected = CollectionUtils.toSet(expectedNames);
        Set<String> result = CollectionUtils.toSet(Iterables.transform(Set.of(aggr), Privilege::getName));

        assertEquals(expected, result);
    }

    @Test
    public void testGetName() {
        assertEquals(JCR_READ_ACCESS_CONTROL, privilege.getName());
    }

    @Test
    public void testIsAbstract() {
        assertFalse(privilege.isAbstract());
        assertFalse(allPrivilege.isAbstract());
        assertFalse(aggrPrivilege.isAbstract());

        assertTrue(abstractPrivilege.isAbstract());
    }

    @Test
    public void testIsAggregate() {
        assertFalse(privilege.isAggregate());

        assertTrue(allPrivilege.isAggregate());
        assertTrue(aggrPrivilege.isAggregate());

        assertFalse(abstractPrivilege.isAggregate());
    }

    @Test
    public void testGetDeclaredAggregatedPrivilegesSimple() {
        assertAggregation(privilege.getDeclaredAggregatePrivileges());
        assertAggregation(aggrPrivilege.getDeclaredAggregatePrivileges(), JCR_NODE_TYPE_MANAGEMENT, JCR_WRITE);
    }


    @Test
    public void testGetAggregatedPrivileges() {
        assertAggregation(privilege.getAggregatePrivileges());
        assertAggregation(aggrPrivilege.getAggregatePrivileges(),
                JCR_NODE_TYPE_MANAGEMENT,
                JCR_WRITE, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
                JCR_MODIFY_PROPERTIES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES);
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(privilege, privilege);
        assertEquals(privilege, getPrivilegeManager(root).getPrivilege(privilege.getName()));
    }

    @Test
    public void testNotEquals() {
        assertNotEquals(privilege, aggrPrivilege);
        assertNotEquals(allPrivilege, privilege);

        final PrivilegeDefinition def = new PrivilegeDefinitionReader(root).readDefinition(privilege.getName());
        assertNotNull(def);
        assertNotEquals(privilege, new Privilege() {

            @Override
            public String getName() {
                return def.getName();
            }

            @Override
            public boolean isAbstract() {
                return def.isAbstract();
            }

            @Override
            public boolean isAggregate() {
                return !def.getDeclaredAggregateNames().isEmpty();
            }

            @Override
            public Privilege[] getDeclaredAggregatePrivileges() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Privilege[] getAggregatePrivileges() {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Test
    public void testToString() {
        PrivilegeDefinition def = new PrivilegeDefinitionReader(root).readDefinition(privilege.getName());
        assertEquals(def.getName(), privilege.toString());
    }

    @Test
    public void testInvalidDeclaredAggregate() throws Exception {
        Tree privilegeDefs = root.getTree(PRIVILEGES_PATH);
        Tree privDef = TreeUtil.addChild(privilegeDefs, "test", NT_REP_PRIVILEGE);
        privDef.setProperty(REP_AGGREGATES, ImmutableList.of(JCR_READ, "invalid"), Type.NAMES);

        Privilege p = getPrivilegeManager(root).getPrivilege("test");
        assertAggregation(p.getDeclaredAggregatePrivileges(), JCR_READ);
    }

    @Test
    public void testCyclicDeclaredAggregate() throws Exception {
        Tree privilegeDefs = root.getTree(PRIVILEGES_PATH);
        Tree privDef = TreeUtil.addChild(privilegeDefs, "test", NT_REP_PRIVILEGE);
        privDef.setProperty(REP_AGGREGATES, ImmutableList.of(JCR_READ, "test"), Type.NAMES);

        Privilege p = getPrivilegeManager(root).getPrivilege("test");
        assertAggregation(p.getDeclaredAggregatePrivileges(), JCR_READ);
    }
}
