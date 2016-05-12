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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import java.security.Principal;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EmptyPrincipalProviderTest extends AbstractPrincipalTest {

    private String exPrincName;
    private Principal testPrincipal;

    @Override
    public void before() throws Exception {
        super.before();

        assertFalse(principalProvider instanceof ExternalGroupPrincipalProvider);

        exPrincName = getUserManager(root).getAuthorizable(USER_ID).getProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES)[0].getString();
        testPrincipal = getTestUser().getPrincipal();
    }

    @Nonnull
    @Override
    PrincipalProvider createPrincipalProvider() {
        return new ExternalPrincipalConfiguration().getPrincipalProvider(root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testGetPrincipal() {
        assertNull(principalProvider.getPrincipal(exPrincName));
        assertNull(principalProvider.getPrincipal(testPrincipal.getName()));
    }

    @Test
    public void testGetGroupMembership() {
        assertTrue(principalProvider.getGroupMembership(new PrincipalImpl(exPrincName)).isEmpty());
        assertTrue(principalProvider.getGroupMembership(testPrincipal).isEmpty());
    }

    @Test
    public void testGetPrincipals() throws Exception {
        assertTrue(principalProvider.getPrincipals(USER_ID).isEmpty());
        assertTrue(principalProvider.getPrincipals(getTestUser().getID()).isEmpty());
    }

    @Test
    public void testFindPrincipalsByHint() {
        assertFalse(principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_ALL).hasNext());
    }

    @Test
    public void testFindPrincipalsByType() {
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL).hasNext());
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP).hasNext());
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP).hasNext());
    }
}