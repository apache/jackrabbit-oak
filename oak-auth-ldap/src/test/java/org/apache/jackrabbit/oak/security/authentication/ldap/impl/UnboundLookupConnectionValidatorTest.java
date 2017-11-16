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

import org.apache.directory.api.ldap.model.constants.SchemaConstants;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class UnboundLookupConnectionValidatorTest {

    private UnboundLookupConnectionValidator validator = new UnboundLookupConnectionValidator();

    @Test
    public void testValidateThrowsException() throws Exception {
        LdapConnection connection = Mockito.mock(LdapConnection.class);
        doThrow(LdapException.class).when(connection).lookup(Dn.ROOT_DSE, SchemaConstants.NO_ATTRIBUTE);

        assertFalse(validator.validate(connection));
    }

    @Test
    public void testValidateNotConnected() {
        LdapConnection connection = Mockito.mock(LdapConnection.class);
        when(connection.isConnected()).thenReturn(false);

        assertFalse(validator.validate(connection));
    }

    @Test
    public void testValidateLookupReturnsNull() throws Exception {
        LdapConnection connection = Mockito.mock(LdapConnection.class);
        when(connection.isConnected()).thenReturn(true);
        when(connection.lookup(Dn.ROOT_DSE, SchemaConstants.NO_ATTRIBUTE)).thenReturn(null);

        assertFalse(validator.validate(connection));
    }

    @Test
    public void testValidateNotConnectedLookupReturnsNull() throws Exception {
        LdapConnection connection = Mockito.mock(LdapConnection.class);
        when(connection.isConnected()).thenReturn(false);
        when(connection.lookup(Dn.ROOT_DSE, SchemaConstants.NO_ATTRIBUTE)).thenReturn(null);

        assertFalse(validator.validate(connection));
    }

    @Test
    public void testValidate() throws Exception {
        LdapConnection connection = Mockito.mock(LdapConnection.class);
        when(connection.isConnected()).thenReturn(true);
        when(connection.lookup(Dn.ROOT_DSE, SchemaConstants.NO_ATTRIBUTE)).thenReturn(Mockito.mock(Entry.class));

        assertTrue(validator.validate(connection));
    }
}