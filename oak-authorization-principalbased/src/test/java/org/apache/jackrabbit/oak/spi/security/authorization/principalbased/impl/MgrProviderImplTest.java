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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class MgrProviderImplTest extends AbstractPrincipalBasedTest {

    private MgrProviderImpl mgrProvider;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        mgrProvider = (MgrProviderImpl) getMgrProvider(root);
    }

    @Test
    public void testGetContext() {
        assertSame(getConfig(AuthorizationConfiguration.class).getContext(), mgrProvider.getContext());
    }

    @Test
    public void testGetNamePathMapper() {
        assertSame(getNamePathMapper(), mgrProvider.getNamePathMapper());
    }

    @Test
    public void testGetNamePathMapper2() {
        MgrProvider mp = new MgrProviderImpl(mock(PrincipalBasedAuthorizationConfiguration.class));
        assertSame(NamePathMapper.DEFAULT, mp.getNamePathMapper());
    }

    @Test
    public void testGetRoot() {
        assertSame(root, mgrProvider.getRoot());
    }

    @Test(expected = IllegalStateException.class)
    public void testRootNotInitialized() {
        MgrProvider mp = new MgrProviderImpl(mock(PrincipalBasedAuthorizationConfiguration.class));
        mp.getRoot();
    }

    @Test
    public void testReset() {
        MgrProvider mp = new MgrProviderImpl(mock(PrincipalBasedAuthorizationConfiguration.class));
        Root r = mock(Root.class);
        NamePathMapper mapper = mock(NamePathMapper.class);
        mp.reset(r, mapper);
        assertSame(r, mp.getRoot());
        assertSame(mapper, mp.getNamePathMapper());
    }

    @Test
    public void testGetPrincipalManager() {
        PrincipalManager pm = mgrProvider.getPrincipalManager();
        assertSame(pm, mgrProvider.getPrincipalManager());

        mgrProvider.reset(root, getNamePathMapper());
        assertNotSame(pm, mgrProvider.getPrincipalManager());
    }

    @Test
    public void testGetPrivilegeManager() {
        PrivilegeManager pm = mgrProvider.getPrivilegeManager();
        assertSame(pm, mgrProvider.getPrivilegeManager());

        mgrProvider.reset(root, getNamePathMapper());
        assertNotSame(pm, mgrProvider.getPrivilegeManager());
    }

    @Test
    public void testGetPrivilegeBitsProvider() {
        PrivilegeBitsProvider pbp = mgrProvider.getPrivilegeBitsProvider();
        assertSame(pbp, mgrProvider.getPrivilegeBitsProvider());

        mgrProvider.reset(root, getNamePathMapper());
        assertNotSame(pbp, mgrProvider.getPrivilegeBitsProvider());
    }

    @Test
    public void testGetRestrictionProvider() {
        RestrictionProvider rp = mgrProvider.getRestrictionProvider();
        assertSame(rp, mgrProvider.getRestrictionProvider());

        mgrProvider.reset(root, getNamePathMapper());
        assertSame(rp, mgrProvider.getRestrictionProvider());
    }

    @Test
    public void testGetSecurityProvider() {
        assertSame(getSecurityProvider(), mgrProvider.getSecurityProvider());
        assertSame(mgrProvider.getSecurityProvider(), mgrProvider.getSecurityProvider());
    }

    @Test
    public void testGetTreeProvider() {
        assertSame(getTreeProvider(), mgrProvider.getTreeProvider());
        assertSame(mgrProvider.getTreeProvider(), mgrProvider.getTreeProvider());
    }

    @Test
    public void testGetRootProvider() {
        assertSame(getRootProvider(), mgrProvider.getRootProvider());
        assertSame(mgrProvider.getRootProvider(), mgrProvider.getRootProvider());
    }
}