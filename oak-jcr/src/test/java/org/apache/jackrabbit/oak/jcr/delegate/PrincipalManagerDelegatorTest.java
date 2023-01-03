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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalQueryManager;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.security.Principal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;

public class PrincipalManagerDelegatorTest extends AbstractDelegatorTest {

    private final SessionDelegate sessionDelegate = mockSessionDelegate();
    private final PrincipalManager principalManager = mock(PrincipalManager.class);
    private final Principal principal = mock(Principal.class);

    private final PrincipalManagerDelegator delegator = new PrincipalManagerDelegator(sessionDelegate, principalManager);

    private static void verifySavePerform(@NotNull SessionDelegate sessionDelegate, int times) throws RepositoryException {
        verify(sessionDelegate, times(times)).safePerform(any(SessionOperation.class));
        verify(sessionDelegate, times(times)).perform(any(SessionOperation.class));
    }
    
    @Test
    public void testHasPrincipal() throws Exception {
        delegator.hasPrincipal("name");
        
        verify(principalManager).hasPrincipal("name");
        verifySavePerform(sessionDelegate, 1);
        verifyNoMoreInteractions(principalManager, sessionDelegate);
    }
    
    @Test
    public void testGetPrincipal() throws Exception {
        delegator.getPrincipal("name");

        verify(principalManager).getPrincipal("name");
        verify(sessionDelegate).safePerformNullable(any(SessionOperation.class));
        verify(sessionDelegate).performNullable(any(SessionOperation.class));
        verifyNoMoreInteractions(principalManager, sessionDelegate);
    }
    
    @Test
    public void testFindPrincipals() throws Exception {
        PrincipalIterator it = mock(PrincipalIterator.class);
        doReturn(it).when(principalManager).findPrincipals(anyString(), anyInt());
        
        String simpleFilter = "simpleFilter";
        delegator.findPrincipals(simpleFilter);
        delegator.findPrincipals(simpleFilter, PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        delegator.findPrincipals(simpleFilter, true, PrincipalManager.SEARCH_TYPE_GROUP, 6, 0);
        
        verify(principalManager).findPrincipals(simpleFilter);
        verify(principalManager).findPrincipals(simpleFilter, PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        verify(principalManager).findPrincipals(simpleFilter, PrincipalManager.SEARCH_TYPE_GROUP);
        verify(it).skip(6);
        verifySavePerform(sessionDelegate, 3);
        verifyNoMoreInteractions(principalManager, sessionDelegate);
    }

    @Test
    public void testFindPrincipalsOnPrincipalQueryManager() throws Exception {
        PrincipalManager pm = mock(PrincipalManager.class, withSettings().extraInterfaces(PrincipalQueryManager.class));
        PrincipalManagerDelegator del = new PrincipalManagerDelegator(sessionDelegate, pm);
        del.findPrincipals("simpleFilter", false, PrincipalManager.SEARCH_TYPE_ALL, 25, 2);

        verify((PrincipalQueryManager) pm).findPrincipals("simpleFilter", false, PrincipalManager.SEARCH_TYPE_ALL, 25, 2);

        verifySavePerform(sessionDelegate, 1);
        verifyNoMoreInteractions(principalManager, sessionDelegate);
    }
    
    @Test
    public void testGetPrincipals() throws Exception {
        delegator.getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);

        verify(principalManager).getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
        verifySavePerform(sessionDelegate, 1);
        verifyNoMoreInteractions(principalManager, sessionDelegate);
    }
    
    @Test
    public void testGetGroupMembership() throws Exception {
        delegator.getGroupMembership(principal);

        verify(principalManager).getGroupMembership(principal);
        verifySavePerform(sessionDelegate, 1);
        verifyNoMoreInteractions(principalManager, sessionDelegate);
    }
    
    @Test
    public void testGetEveryone() throws Exception {
        delegator.getEveryone();
        
        verify(principalManager).getEveryone();
        verifySavePerform(sessionDelegate, 1);
        verifyNoMoreInteractions(principalManager, sessionDelegate);
    }
}