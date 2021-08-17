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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;

import javax.jcr.security.Privilege;

import static org.junit.Assert.assertArrayEquals;

public class PrivilegeCollectionDefaultTest extends AbstractEvaluationTest {
    
    public void testEmptyCollection() throws Exception {
        PrivilegeCollection pc = new PrivilegeCollection.Default(new Privilege[0], acMgr);
        assertArrayEquals(new Privilege[0], pc.getPrivileges());
        assertTrue(pc.includes());
        assertFalse(pc.includes(Privilege.JCR_ALL));
        assertFalse(pc.includes(PrivilegeConstants.JCR_READ));
        assertFalse(pc.includes(Privilege.JCR_MODIFY_ACCESS_CONTROL));
    }
    
    public void testJcrAllCollection() throws Exception {
        Privilege[] jcrAll = privilegesFromName(Privilege.JCR_ALL);
        PrivilegeCollection pc = new PrivilegeCollection.Default(jcrAll, acMgr);
        assertArrayEquals(jcrAll, pc.getPrivileges());
        assertTrue(pc.includes());
        assertTrue(pc.includes(Privilege.JCR_ALL));
        assertTrue(pc.includes(PrivilegeConstants.REP_READ_NODES));
        assertTrue(pc.includes(Privilege.JCR_LIFECYCLE_MANAGEMENT));
        assertTrue(pc.includes(Privilege.JCR_WRITE));
    }
    
    public void testNonAggregated() throws Exception {
        Privilege[] privs = privilegesFromNames(new String[] {Privilege.JCR_NODE_TYPE_MANAGEMENT, Privilege.JCR_REMOVE_NODE, Privilege.JCR_READ_ACCESS_CONTROL});
        PrivilegeCollection pc = new PrivilegeCollection.Default(privs, acMgr);
        assertArrayEquals(privs, pc.getPrivileges());
        assertTrue(pc.includes());
        assertFalse(pc.includes(Privilege.JCR_ALL));
        assertFalse(pc.includes(PrivilegeConstants.REP_READ_PROPERTIES));
        assertFalse(pc.includes(Privilege.JCR_REMOVE_CHILD_NODES));
        assertTrue(pc.includes(Privilege.JCR_NODE_TYPE_MANAGEMENT, Privilege.JCR_REMOVE_NODE, Privilege.JCR_READ_ACCESS_CONTROL));
        assertTrue(pc.includes(Privilege.JCR_REMOVE_NODE));
    }

    public void testAggregated() throws Exception {
        Privilege[] privs = privilegesFromNames(new String[] {Privilege.JCR_READ, Privilege.JCR_MODIFY_PROPERTIES, PrivilegeConstants.REP_WRITE});
        PrivilegeCollection pc = new PrivilegeCollection.Default(privs, acMgr);
        assertArrayEquals(privs, pc.getPrivileges());
        assertTrue(pc.includes());
        assertFalse(pc.includes(Privilege.JCR_ALL));
        assertFalse(pc.includes(PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT));
        assertTrue(pc.includes(Privilege.JCR_READ, Privilege.JCR_MODIFY_PROPERTIES, PrivilegeConstants.REP_WRITE));
        assertTrue(pc.includes(PrivilegeConstants.REP_READ_PROPERTIES, PrivilegeConstants.REP_READ_NODES));
        assertTrue(pc.includes(PrivilegeConstants.REP_ADD_PROPERTIES, PrivilegeConstants.REP_REMOVE_PROPERTIES));
        assertTrue(pc.includes(Privilege.JCR_REMOVE_CHILD_NODES, Privilege.JCR_MODIFY_PROPERTIES, Privilege.JCR_NODE_TYPE_MANAGEMENT));
    }
}