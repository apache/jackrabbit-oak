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
package org.apache.jackrabbit.oak.spi.security.authentication;

import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AuthInfoImplNullTest {

    private final AuthInfoImpl authInfo = new AuthInfoImpl(null, null, null);

    @Test
    public void testGetUserID() {
        assertNull(authInfo.getUserID());
    }

    @Test
    public void testGetAttributeNames() {
        String[] attrNames = authInfo.getAttributeNames();
        assertNotNull(attrNames);
        assertEquals(0, attrNames.length);
    }

    @Test
    public void testGetAttribute() {
        assertNull(authInfo.getAttribute("any"));
    }

    @Test
    public void testGetPrincipals() {
        assertTrue(authInfo.getPrincipals().isEmpty());
    }

    @Test
    public void testToString() {
        assertNotNull(authInfo.toString());
    }

    @Test
    public void testCreateAuthInfoFromEmptySubject() {
        AuthInfo info = AuthInfoImpl.createFromSubject(new Subject());
        assertNull(info.getUserID());
        assertEquals(0, info.getAttributeNames().length);
        assertTrue(info.getPrincipals().isEmpty());

    }
}