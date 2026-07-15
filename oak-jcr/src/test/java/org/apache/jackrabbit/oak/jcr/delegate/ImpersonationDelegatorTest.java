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

import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.security.auth.Subject;
import java.security.Principal;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ImpersonationDelegatorTest extends AbstractDelegatorTest {
    
    private final SessionDelegate sessionDelegate = mockSessionDelegate();
    private final Impersonation impersonation = mock(Impersonation.class);
    private final ImpersonationDelegator delegator = mockDelegator(sessionDelegate, impersonation);

    private static ImpersonationDelegator mockDelegator(@NotNull SessionDelegate sd, @NotNull Impersonation impersonation) {
        Impersonation i = ImpersonationDelegator.wrap(sd, impersonation);
        assertTrue(i instanceof ImpersonationDelegator);
        return spy((ImpersonationDelegator) i);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrapDelegator() {
        ImpersonationDelegator.wrap(sessionDelegate, delegator);
    }
    
    @Test
    public void testGetImpersonators() throws Exception {
        delegator.getImpersonators();
        
        verify(impersonation).getImpersonators();
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(impersonation, sessionDelegate);
    }
    
    @Test
    public void testGrantImpersonation() throws Exception {
        Principal principal = mock(Principal.class);
        delegator.grantImpersonation(principal);

        verify(impersonation).grantImpersonation(principal);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(impersonation, sessionDelegate);
    }
    
    @Test
    public void testRevokeImpersonation() throws Exception {
        Principal principal = mock(Principal.class);
        delegator.revokeImpersonation(principal);

        verify(impersonation).revokeImpersonation(principal);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(impersonation, sessionDelegate);
    }

    @Test
    public void testAllows() throws Exception {
        Subject subject = new Subject();
        delegator.allows(subject);

        verify(impersonation).allows(subject);
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(impersonation, sessionDelegate);
    }
}