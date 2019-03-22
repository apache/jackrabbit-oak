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
package org.apache.jackrabbit.oak.spi.security.principal;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrincipalProviderTest {

    private PrincipalProvider pp = mock(PrincipalProvider.class);

    @Test
    public void testGetItemBasedPrincipal() {
        doCallRealMethod().when(pp).getItemBasedPrincipal("/some/path");
        assertNull(pp.getItemBasedPrincipal("/some/path"));
    }

    @Test
    public void testGetGroupMembership() {
        Principal p = mock(Principal.class);
        doCallRealMethod().when(pp).getGroupMembership(p);
        assertTrue(pp.getGroupMembership(p).isEmpty());
    }

    @Test
    public void testGetMembershipPrincipals() {
        Principal p = mock(Principal.class);
        doCallRealMethod().when(pp).getMembershipPrincipals(p);
        assertTrue(pp.getMembershipPrincipals(p).isEmpty());
    }



    @Test(expected = IllegalArgumentException.class)
    public void testNegativeOffset() {
        doCallRealMethod().when(pp).findPrincipals("hint", true, PrincipalManager.SEARCH_TYPE_GROUP, -1, 12);
        pp.findPrincipals("hint", true, PrincipalManager.SEARCH_TYPE_GROUP, -1, 12);
    }
}