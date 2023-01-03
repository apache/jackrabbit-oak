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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ADD_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class AbstractPrivilegeCollectionTest extends AbstractAccessControlTest {
    
    private @NotNull AbstractPrivilegeCollection createPrivilegeCollection(@NotNull String... privilegeNames) {
        return new AbstractPrivilegeCollection(getBitsProvider().getBits(privilegeNames)) {
            @Override
            @NotNull PrivilegeBitsProvider getPrivilegeBitsProvider() {
                return AbstractPrivilegeCollectionTest.this.getBitsProvider();
            }

            @Override
            @NotNull NamePathMapper getNamePathMapper() {
                return AbstractPrivilegeCollectionTest.this.getNamePathMapper();
            }

            @Override
            public Privilege[] getPrivileges() throws RepositoryException {
                throw new RepositoryException("not implemented");
            }
        };
    }
    
    @Test
    public void testIncludes() throws Exception {
        AbstractPrivilegeCollection apc = createPrivilegeCollection(JCR_WRITE, JCR_READ);
        
        assertTrue(apc.includes());
        assertTrue(apc.includes(JCR_WRITE, JCR_READ));
        assertTrue(apc.includes(JCR_WRITE));
        assertTrue(apc.includes(JCR_READ));
        assertTrue(apc.includes(REP_READ_NODES, REP_READ_NODES));

        assertFalse(apc.includes(JCR_MODIFY_ACCESS_CONTROL));
        assertFalse(apc.includes(REP_WRITE));
    }
    
    @Test
    public void testEquals() {
        PrivilegeCollection apc = createPrivilegeCollection(JCR_READ_ACCESS_CONTROL, JCR_READ);

        assertEquals(apc, createPrivilegeCollection(JCR_READ, JCR_READ_ACCESS_CONTROL));
        assertEquals(apc, createPrivilegeCollection(REP_READ_PROPERTIES, REP_READ_NODES, JCR_READ_ACCESS_CONTROL));
        assertEquals(apc, createPrivilegeCollection(REP_READ_PROPERTIES, REP_READ_NODES, JCR_READ, JCR_READ_ACCESS_CONTROL));
        assertEquals(apc, apc);
        
        assertNotEquals(apc, createPrivilegeCollection(JCR_READ_ACCESS_CONTROL));
        assertNotEquals(apc, createPrivilegeCollection(JCR_READ));
        assertNotEquals(apc, createPrivilegeCollection(JCR_READ_ACCESS_CONTROL, JCR_READ, JCR_MODIFY_PROPERTIES));
        // different impl
        assertNotEquals(apc, new PrivilegeCollection() {
            @Override
            public Privilege[] getPrivileges() {
                return new Privilege[0];
            }

            @Override
            public boolean includes(@NotNull String... privilegeNames) {
                return false;
            }
        });
    }
    
    @Test
    public void testHashCode() {
        AbstractPrivilegeCollection apc = createPrivilegeCollection(JCR_ADD_CHILD_NODES, JCR_MODIFY_PROPERTIES);
        
        assertEquals(getBitsProvider().getBits(JCR_ADD_CHILD_NODES, JCR_MODIFY_PROPERTIES).hashCode(), apc.hashCode());
        assertEquals(apc.hashCode(), createPrivilegeCollection(JCR_ADD_CHILD_NODES, JCR_MODIFY_PROPERTIES).hashCode());
        
        assertNotEquals(apc.hashCode(), createPrivilegeCollection(JCR_ADD_CHILD_NODES).hashCode());
        assertNotEquals(apc.hashCode(), createPrivilegeCollection(JCR_MODIFY_PROPERTIES).hashCode());
        assertNotEquals(apc.hashCode(), createPrivilegeCollection(JCR_READ).hashCode());
    }
    
    @Test
    public void testEmpty() throws RepositoryException {
        AbstractPrivilegeCollection apc = createPrivilegeCollection();
        
        assertTrue(apc.includes());
        assertFalse(apc.includes(REP_READ_NODES));

        assertEquals(PrivilegeBits.EMPTY.hashCode(), apc.hashCode());
        
        assertEquals(apc, createPrivilegeCollection());
        assertNotEquals(apc, createPrivilegeCollection(JCR_READ));

    }

}