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

import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionValidator;
import org.apache.directory.ldap.client.api.LookupLdapConnectionValidator;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PoolableUnboundConnectionFactoryTest {

    PoolableUnboundConnectionFactory factory = new PoolableUnboundConnectionFactory(new LdapConnectionConfig());

    @Test
    public void testGetValidator() {
        LdapConnectionValidator validator = factory.getValidator();
        assertTrue(validator instanceof LookupLdapConnectionValidator);
    }

    @Test
    public void testSetValidator() {
        LdapConnectionValidator validator = Mockito.mock(LdapConnectionValidator.class);
        factory.setValidator(validator);

        assertEquals(validator, factory.getValidator());
    }
}