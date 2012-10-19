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
package org.apache.jackrabbit.oak.security.principal;

import java.security.Principal;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * PrincipalProviderImplTest...
 */
public class PrincipalProviderImplTest extends AbstractSecurityTest {

    @Test
    public void testGetPrincipals() throws Exception {
        Root root = admin.getLatestRoot();
        PrincipalProviderImpl principalProvider =
                new PrincipalProviderImpl(root, securityProvider.getUserConfiguration(), NamePathMapper.DEFAULT);

        String adminId = admin.getAuthInfo().getUserID();
        Set<? extends Principal> principals = principalProvider.getPrincipals(adminId);

        assertNotNull(principals);
        assertFalse(principals.isEmpty());
        assertTrue(principals.contains(EveryonePrincipal.getInstance()));

        boolean containsAdminPrincipal = false;
        for (Principal principal : principals) {
            assertNotNull(principalProvider.getPrincipal(principal.getName()));
            if (principal instanceof AdminPrincipal) {
                containsAdminPrincipal = true;
            }
        }
        assertTrue(containsAdminPrincipal);
    }
}