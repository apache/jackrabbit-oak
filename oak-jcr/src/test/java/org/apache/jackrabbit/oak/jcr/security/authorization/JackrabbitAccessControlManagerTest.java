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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.test.NotExecutableException;

import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;

public class JackrabbitAccessControlManagerTest extends AbstractEvaluationTest {
    
    private JackrabbitAccessControlManager jrAcMgr;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        if (!(acMgr instanceof JackrabbitAccessControlManager)) {
            throw new NotExecutableException("JackrabbitAccessControlManager expected");
        }
        jrAcMgr = (JackrabbitAccessControlManager) acMgr;
    }
    
    public void testGetPrivilegeCollection() throws Exception {
        PrivilegeCollection pc = jrAcMgr.getPrivilegeCollection(path);
        assertTrue(pc.includes(Privilege.JCR_ALL));
        assertTrue(pc.includes(Privilege.JCR_READ, PrivilegeConstants.REP_READ_PROPERTIES));
        assertArrayEquals(privilegesFromName(Privilege.JCR_ALL), pc.getPrivileges());
    }
    
    public void testGetPrivilegeCollectionInvalidPath() throws Exception {
        try {
            jrAcMgr.getPrivilegeCollection("/invalid");
            fail();
        } catch (PathNotFoundException e) {
            // success
        }
    }

    public void testGetPrivilegeCollectionEmptyPrincipalSet() throws Exception {
        PrivilegeCollection pc = jrAcMgr.getPrivilegeCollection(path, Collections.emptySet());
        assertFalse(pc.includes(Privilege.JCR_ALL));
        assertFalse(pc.includes(Privilege.JCR_READ, PrivilegeConstants.REP_READ_PROPERTIES));
        assertArrayEquals(new Privilege[0], pc.getPrivileges());
    }

    public void testGetPrivilegeCollectionPrincipalSet() throws Exception {
        PrivilegeCollection pc = jrAcMgr.getPrivilegeCollection(path, Collections.singleton(EveryonePrincipal.getInstance()));
        assertFalse(pc.includes(Privilege.JCR_ALL));
        assertTrue(pc.includes(Privilege.JCR_READ, PrivilegeConstants.REP_READ_PROPERTIES));
        assertArrayEquals(privilegesFromName(Privilege.JCR_READ), pc.getPrivileges());
    }
    
    public void testGetPrivilegeCollectionTestSession() throws Exception {
        PrivilegeCollection pc = ((JackrabbitAccessControlManager) testAcMgr).getPrivilegeCollection(path);
        assertFalse(pc.includes(Privilege.JCR_ALL));
        assertTrue(pc.includes(Privilege.JCR_READ, PrivilegeConstants.REP_READ_PROPERTIES));
        assertArrayEquals(privilegesFromName(Privilege.JCR_READ), pc.getPrivileges());
    }

    public void testGetPrivilegeCollectionTestSessionPrincipalSet() throws Exception {
        try {
            ((JackrabbitAccessControlManager) testAcMgr).getPrivilegeCollection(path, Collections.singleton(EveryonePrincipal.getInstance()));
            fail("AccessDeniedException expected");
        } catch (AccessDeniedException e) {
            // success
        }
    }
    
    public void testPrivilegeCollectionFromNames() throws Exception {
        PrivilegeCollection pc = jrAcMgr.privilegeCollectionFromNames(Privilege.JCR_READ, Privilege.JCR_WRITE);
        
        assertFalse(pc instanceof PrivilegeCollection.Default);
        Set<Privilege> expected = Set.of(privilegesFromNames(new String[] {Privilege.JCR_READ, Privilege.JCR_WRITE}));
        assertEquals(expected, Set.of(pc.getPrivileges()));
    }

    public void testPrivilegeCollectionFromInvalidNames() throws Exception {
        try {
            PrivilegeCollection pc = jrAcMgr.privilegeCollectionFromNames("invalidPrivilegeName");
            fail("AccessControlException expected");
        } catch (AccessControlException e) {
            // success
        }
    }
}