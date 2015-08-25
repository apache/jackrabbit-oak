/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.upgrade;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ADD_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LOCK_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_NODE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_RETENTION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_ADD_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_ALTER_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_PRIVILEGE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_REMOVE_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_USER_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_WRITE;

public class PrivilegeUpgradeTest extends AbstractRepositoryUpgradeTest {

    @Override
    protected void createSourceContent(Session session) throws Exception {
        JackrabbitWorkspace workspace = (JackrabbitWorkspace) session.getWorkspace();

        NamespaceRegistry registry = workspace.getNamespaceRegistry();
        registry.registerNamespace("test", "http://www.example.org/");

        PrivilegeManager privilegeManager = workspace.getPrivilegeManager();
        privilegeManager.registerPrivilege("test:privilege", false, null);
        privilegeManager.registerPrivilege("test:aggregate", false, new String[] { "jcr:read", "test:privilege" });
        privilegeManager.registerPrivilege("test:privilege2", true, null);
        privilegeManager.registerPrivilege("test:aggregate2", true,
                new String[] { "test:aggregate", "test:privilege2" });
    }

    @Test
    public void verifyPrivileges() throws RepositoryException {
        Set<String> nonAggregatePrivileges = newHashSet(
            REP_READ_NODES, REP_READ_PROPERTIES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES,
            REP_REMOVE_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
            JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL, JCR_NODE_TYPE_MANAGEMENT,
            JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
            JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
            JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_USER_MANAGEMENT,
            REP_INDEX_DEFINITION_MANAGEMENT, "test:privilege", "test:privilege2");

        Map<String, Set<String>> aggregatePrivileges = Maps.newHashMap();
        aggregatePrivileges.put(JCR_READ,
                ImmutableSet.of(REP_READ_NODES, REP_READ_PROPERTIES));
        aggregatePrivileges.put(JCR_MODIFY_PROPERTIES,
                ImmutableSet.of(REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES));
        aggregatePrivileges.put(JCR_WRITE,
                ImmutableSet.of(JCR_MODIFY_PROPERTIES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES,
                        REP_REMOVE_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES,
                        JCR_REMOVE_NODE));
        aggregatePrivileges.put(REP_WRITE,
                ImmutableSet.of(JCR_WRITE, JCR_MODIFY_PROPERTIES, REP_ADD_PROPERTIES,
                        REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES, JCR_ADD_CHILD_NODES,
                        JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE, JCR_NODE_TYPE_MANAGEMENT));
        aggregatePrivileges.put(JCR_ALL,
                ImmutableSet.of(REP_READ_NODES, REP_READ_PROPERTIES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES,
                        REP_REMOVE_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
                        JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL, JCR_NODE_TYPE_MANAGEMENT,
                        JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
                        JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
                        JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_USER_MANAGEMENT,
                        REP_INDEX_DEFINITION_MANAGEMENT, JCR_READ, JCR_MODIFY_PROPERTIES, JCR_WRITE, REP_WRITE,
                        "test:privilege", "test:privilege2", "test:aggregate", "test:aggregate2"));
        aggregatePrivileges.put("test:aggregate",
                ImmutableSet.of(JCR_READ, REP_READ_NODES, REP_READ_PROPERTIES, "test:privilege"));
        aggregatePrivileges.put("test:aggregate2",
                ImmutableSet.of(JCR_READ, REP_READ_NODES, REP_READ_PROPERTIES, "test:privilege", "test:privilege2", "test:aggregate"));

        JackrabbitSession session = createAdminSession();
        try {
            JackrabbitWorkspace workspace = (JackrabbitWorkspace) session.getWorkspace();
            PrivilegeManager manager = workspace.getPrivilegeManager();
            Privilege[] privileges = manager.getRegisteredPrivileges();

            for (Privilege privilege : privileges) {
                if (privilege.isAggregate()) {
                    Set<String> expected = aggregatePrivileges.remove(privilege.getName());
                    if (expected != null) {
                        String[] actual = getNames(privilege.getAggregatePrivileges());
                        assertTrue("Miss match in aggregate privilege " + privilege.getName() +
                                " expected " + expected +
                                " actual " + Arrays.toString(actual),
                            newHashSet(expected).equals(newHashSet(actual)));
                    }
                } else {
                    nonAggregatePrivileges.remove(privilege.getName());
                }
            }

            assertTrue("Missing non aggregate privileges: " + nonAggregatePrivileges, nonAggregatePrivileges.isEmpty());
            assertTrue("Missing aggregate privileges: " + aggregatePrivileges.keySet(), aggregatePrivileges.isEmpty());
        }
        finally {
            session.logout();
        }
    }

    private static String[] getNames(Privilege[] privileges) {
        String[] names = new String[privileges.length];
        for (int i = 0; i < privileges.length; i++) {
            names[i] = privileges[i].getName();
        }
        return names;
    }

    @Test
    public void verifyCustomPrivileges() throws Exception {
        JackrabbitSession session = createAdminSession();
        try {
            JackrabbitWorkspace workspace = (JackrabbitWorkspace) session.getWorkspace();
            PrivilegeManager manager = workspace.getPrivilegeManager();

            Privilege privilege = manager.getPrivilege("test:privilege");
            assertNotNull(privilege);
            assertFalse(privilege.isAbstract());
            assertFalse(privilege.isAggregate());
            assertEquals(0, privilege.getDeclaredAggregatePrivileges().length);

            Privilege privilege2 = manager.getPrivilege("test:privilege2");
            assertNotNull(privilege2);
            assertTrue(privilege2.isAbstract());
            assertFalse(privilege2.isAggregate());
            assertEquals(0, privilege.getDeclaredAggregatePrivileges().length);

            Privilege aggregate = manager.getPrivilege("test:aggregate");
            assertNotNull(aggregate);
            assertFalse(aggregate.isAbstract());
            assertTrue(aggregate.isAggregate());
            List<Privilege> agg = ImmutableList.copyOf(aggregate.getDeclaredAggregatePrivileges());
            assertEquals(2, agg.size());
            assertTrue(agg.contains(privilege));
            assertTrue(agg.contains(manager.getPrivilege(JCR_READ)));

            Privilege aggregate2 = manager.getPrivilege("test:aggregate2");
            assertNotNull(aggregate2);
            assertTrue(aggregate2.isAbstract());
            assertTrue(aggregate2.isAggregate());
            List<Privilege> agg2 = ImmutableList.copyOf(aggregate2.getDeclaredAggregatePrivileges());
            assertEquals(2, agg2.size());
            assertTrue(agg2.contains(aggregate));
            assertTrue(agg2.contains(privilege2));

            Privilege jcrAll = manager.getPrivilege("jcr:all");
            List<Privilege> privileges = asList(jcrAll.getAggregatePrivileges());
            assertTrue(privileges.contains(privilege));
            assertTrue(privileges.contains(privilege2));
            assertTrue(privileges.contains(aggregate));
            assertTrue(privileges.contains(aggregate2));
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyCustomPrivilegeBits() throws Exception {
        JackrabbitSession session = createAdminSession();
        try {
            Node privilegeRoot = session.getNode(PrivilegeConstants.PRIVILEGES_PATH);

            Node testPrivilegeNode = privilegeRoot.getNode("test:privilege");
            long l = getLong(testPrivilegeNode);
            PrivilegeBits pb = readBits(testPrivilegeNode, PrivilegeConstants.REP_BITS);


            Node testPrivilege2Node = privilegeRoot.getNode("test:privilege2");
            long l2 = getLong(testPrivilege2Node);
            PrivilegeBits pb2 = readBits(testPrivilege2Node, PrivilegeConstants.REP_BITS);

            PrivilegeBits nextExpected;
            if (l < l2) {
                assertEquals(PrivilegeBits.NEXT_AFTER_BUILT_INS, pb);
                assertEquals(pb.nextBits(), pb2);
                nextExpected = pb2.nextBits();
            } else {
                assertEquals(PrivilegeBits.NEXT_AFTER_BUILT_INS, pb2);
                assertEquals(pb2.nextBits(), pb);
                nextExpected = pb.nextBits();
            }

            // make sure the next-value has been properly set
            PrivilegeBits nextBits = readBits(privilegeRoot, PrivilegeConstants.REP_NEXT);
            assertEquals(nextExpected, nextBits);

            Node testAggregateNode = privilegeRoot.getNode("test:aggregate");
            PrivilegeBits aggrPb = readBits(testAggregateNode, PrivilegeConstants.REP_BITS);
            PrivilegeBits expected = PrivilegeBits.getInstance(PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), pb).unmodifiable();
            assertEquals(expected, aggrPb);

            Node testAggregate2Node = privilegeRoot.getNode("test:aggregate2");
            PrivilegeBits aggr2Pb = readBits(testAggregate2Node, PrivilegeConstants.REP_BITS);
            PrivilegeBits expected2 = PrivilegeBits.getInstance(aggrPb, pb2).unmodifiable();
            assertEquals(expected2, aggr2Pb);

        } finally {
            session.logout();
        }
    }

    private static PrivilegeBits readBits(Node node, String name) throws RepositoryException {
        return PrivilegeBits.getInstance(PropertyStates.createProperty(name, Arrays.asList(node.getProperty(name).getValues())));
    }

    private static long getLong(Node node) throws RepositoryException {
        return node.getProperty(PrivilegeConstants.REP_BITS).getValues()[0].getLong();
    }
}
