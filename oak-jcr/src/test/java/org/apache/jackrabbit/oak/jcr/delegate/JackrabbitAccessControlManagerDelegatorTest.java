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
package org.apache.jackrabbit.oak.jcr.delegate;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.junit.Test;

import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class JackrabbitAccessControlManagerDelegatorTest extends AbstractDelegatorTest {
    
    private final SessionDelegate sessionDelegate = mockSessionDelegate();
    private final JackrabbitAccessControlManager jrAcMgr = mock(JackrabbitAccessControlManager.class);
    private final Principal principal = mock(Principal.class);
    private final String absPath = "/path";
    
    private final JackrabbitAccessControlManagerDelegator delegator = new JackrabbitAccessControlManagerDelegator(sessionDelegate, jrAcMgr);
    
    @Test
    public void testGetApplicablePoliciesByPrincipal() throws Exception {
        delegator.getApplicablePolicies(principal);
        
        verify(jrAcMgr).getApplicablePolicies(principal);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetPoliciesByPrincipal() throws Exception {
        delegator.getPolicies(principal);

        verify(jrAcMgr).getPolicies(principal);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetEffectivePoliciesByPrincipals() throws Exception {
        delegator.getEffectivePolicies(Collections.singleton(principal));

        verify(jrAcMgr).getEffectivePolicies(Collections.singleton(principal));
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }
    
    @Test
    public void testHasPrivilegesByPrincipals() throws Exception {
        delegator.hasPrivileges(absPath, Collections.singleton(principal), new Privilege[0]);

        verify(jrAcMgr).hasPrivileges(absPath, Collections.singleton(principal), new Privilege[0]);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetPrivilegesByPrincipals() throws Exception {
        delegator.getPrivileges(absPath, Collections.singleton(principal));

        verify(jrAcMgr).getPrivileges(absPath, Collections.singleton(principal));
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetPrivilegeCollectionByPrincipals() throws Exception {
        delegator.getPrivilegeCollection(absPath, Collections.singleton(principal));

        verify(jrAcMgr).getPrivilegeCollection(absPath, Collections.singleton(principal));
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetPrivilegeCollection() throws Exception {
        delegator.getPrivilegeCollection(absPath);

        verify(jrAcMgr).getPrivilegeCollection(absPath);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetSupportedPrivileges() throws Exception {
        delegator.getSupportedPrivileges(absPath);

        verify(jrAcMgr).getSupportedPrivileges(absPath);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testPrivilegeFromName() throws Exception {
        delegator.privilegeFromName("name");

        verify(jrAcMgr).privilegeFromName("name");
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testHasPrivileges() throws Exception {
        delegator.hasPrivileges(absPath, new Privilege[0]);

        verify(jrAcMgr).hasPrivileges(absPath, new Privilege[0]);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetPrivileges() throws Exception {
        delegator.getPrivileges(absPath);

        verify(jrAcMgr).getPrivileges(absPath);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }


    @Test
    public void testGetApplicablePolicies() throws Exception {
        delegator.getApplicablePolicies(absPath);

        verify(jrAcMgr).getApplicablePolicies(absPath);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetPolicies() throws Exception {
        delegator.getPolicies(absPath);

        verify(jrAcMgr).getPolicies(absPath);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        delegator.getEffectivePolicies(absPath);

        verify(jrAcMgr).getEffectivePolicies(absPath);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }

    @Test
    public void testSetPolicy() throws Exception {
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        delegator.setPolicy(absPath, policy);

        verify(jrAcMgr).setPolicy(absPath, policy);
        verify(sessionDelegate).performVoid(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }
    
    @Test
    public void testRemovePolicy() throws Exception {
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        delegator.removePolicy(absPath, policy);

        verify(jrAcMgr).removePolicy(absPath, policy);
        verify(sessionDelegate).performVoid(any(SessionOperation.class));
        verifyNoMoreInteractions(jrAcMgr, sessionDelegate);
        verifyNoInteractions(principal);
    }
}