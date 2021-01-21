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
package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionValidator;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.apache.directory.ldap.client.api.LookupLdapConnectionValidator;
import org.apache.jackrabbit.oak.security.authentication.ldap.LdapServerClassLoader;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class PoolableUnboundConnectionFactoryTest {

    private static LdapServerClassLoader.Proxy PROXY;

    private final LdapConnectionConfig config = spy(new LdapConnectionConfig());
    private final PoolableUnboundConnectionFactory factory = new PoolableUnboundConnectionFactory(config);

    @BeforeClass
    public static void beforeClass() throws Exception {
        LdapServerClassLoader serverClassLoader = LdapServerClassLoader.createServerClassLoader();
        PROXY = serverClassLoader.createAndSetupServer();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        PROXY.tearDown();
    }

    @Test
    public void testGetValidator() {
        LdapConnectionValidator validator = factory.getValidator();
        assertTrue(validator instanceof LookupLdapConnectionValidator);
    }

    @Test
    public void testSetValidator() {
        LdapConnectionValidator validator = mock(LdapConnectionValidator.class);
        factory.setValidator(validator);

        assertEquals(validator, factory.getValidator());
    }

    @Test
    public void testActivateObject() {
        LdapConnection lc = mock(LdapConnection.class);
        factory.activateObject(lc);

        verifyNoInteractions(lc);
        verifyNoInteractions(config);
    }

    @Test
    public void testDestroyObject() throws Exception {
        LdapConnection lc = mock(LdapConnection.class);
        factory.destroyObject(lc);

        verify(lc).close();
        verifyNoInteractions(config);
    }

    @Test
    public void testCreate() throws Exception {
        when(config.getLdapHost()).thenReturn(PROXY.host);
        when(config.getLdapPort()).thenReturn(PROXY.port);

        when(config.isUseTls()).thenReturn(false);
        assertConnection(factory.create(), false);
    }

    @Test
    public void testCreateTlsGuardingConnection() throws Exception {
        when(config.getLdapHost()).thenReturn(PROXY.host);
        when(config.getLdapPort()).thenReturn(PROXY.port);

        when(config.isUseTls()).thenReturn(true);
        LdapConnection lc = factory.create();
        assertConnection(lc, true);
    }

    private static void assertConnection(@NotNull LdapConnection lc, boolean expectTlsGuardingConnection) {
        assertTrue(lc instanceof LdapNetworkConnection);
        assertEquals(expectTlsGuardingConnection, lc.getClass().getSimpleName().equals("TlsGuardingConnection"));
    }

    @Test
    public void testWrap() {
        LdapConnection lc = mock(LdapConnection.class);
        assertTrue(factory.wrap(lc) instanceof DefaultPooledObject);
        verifyNoInteractions(lc);
    }

    @Test
    public void testPassivateObject() {
        LdapConnection lc = mock(LdapConnection.class);
        factory.passivateObject(lc);
        verifyNoInteractions(lc);
    }

    @Test
    public void testValidateObjectMissingValidator() {
        LdapConnection lc = mock(LdapConnection.class);

        factory.setValidator(null);
        assertTrue(factory.validateObject(lc));
        verifyNoInteractions(lc);
    }

    @Test
    public void testValidateObjectWithValidator() {
        LdapConnection lc = mock(LdapConnection.class);

        LdapConnectionValidator validator = mock(LdapConnectionValidator.class);
        factory.setValidator(validator);
        assertFalse(factory.validateObject(lc));

        when(validator.validate(lc)).thenReturn(true);
        assertTrue(factory.validateObject(lc));

        verify(validator, times(2)).validate(lc);
    }
}