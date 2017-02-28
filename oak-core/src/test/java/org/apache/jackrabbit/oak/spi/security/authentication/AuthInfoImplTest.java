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

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AuthInfoImplTest {

    private static final String USER_ID = "userId";
    private static final Map<String, String> ATTRIBUTES = ImmutableMap.of("attr", "value");
    private static final Set<Principal> PRINCIPALS = ImmutableSet.<Principal>of(new PrincipalImpl("principalName"));

    private final AuthInfoImpl authInfo = new AuthInfoImpl(USER_ID, ATTRIBUTES, PRINCIPALS);

    @Test
    public void testGetUserID() {
        assertEquals(USER_ID, authInfo.getUserID());
    }

    @Test
    public void testGetAttributeNames() {
        String[] attrNames = authInfo.getAttributeNames();
        assertArrayEquals(ATTRIBUTES.keySet().toArray(new String[ATTRIBUTES.size()]), attrNames);
    }

    @Test
    public void testGetAttribute() {
        for (String attrName : ATTRIBUTES.keySet()) {
            assertEquals(ATTRIBUTES.get(attrName), authInfo.getAttribute(attrName));
        }
    }

    @Test
    public void testGetPrincipals() {
        assertEquals(PRINCIPALS, authInfo.getPrincipals());
    }

    @Test
    public void testToString() {
        assertNotNull(authInfo.toString());
    }

    @Test
    public void testCreateFromSubjectWithAuthInfo() {
        Subject subject = new Subject();
        subject.getPublicCredentials().add(authInfo);

        AuthInfo info = AuthInfoImpl.createFromSubject(subject);
        assertEquals(USER_ID, info.getUserID());
        assertEquals(PRINCIPALS, info.getPrincipals());
        assertArrayEquals(authInfo.getAttributeNames(), info.getAttributeNames());
    }

    @Test
    public void testCreateFromSubjectWithPrincipals() {
        Subject subject = new Subject();
        subject.getPrincipals().addAll(PRINCIPALS);

        AuthInfo info = AuthInfoImpl.createFromSubject(subject);
        assertNull(info.getUserID());
        assertEquals(PRINCIPALS, info.getPrincipals());
        assertEquals(0, info.getAttributeNames().length);
    }

    @Test
    public void testCreateFromSubjectWithSimpleCredentials() {
        Subject subject = new Subject();
        subject.getPublicCredentials().add(new SimpleCredentials(USER_ID, new char[0]));

        AuthInfo info = AuthInfoImpl.createFromSubject(subject);
        assertEquals(USER_ID, info.getUserID());
        assertTrue(info.getPrincipals().isEmpty());
        assertEquals(0, info.getAttributeNames().length);
    }

    @Test
    public void testCreateFromSubjectWithPrivateSimpleCredentials() {
        Subject subject = new Subject();
        subject.getPrivateCredentials().add(new SimpleCredentials(USER_ID, new char[0]));

        AuthInfo info = AuthInfoImpl.createFromSubject(subject);
        assertNull(info.getUserID());
        assertTrue(info.getPrincipals().isEmpty());
        assertEquals(0, info.getAttributeNames().length);
    }


    @Test
    public void testCreateFromSubjectWithAnyCredentials() {
        Subject subject = new Subject();
        subject.getPublicCredentials().add(new Credentials() {
        });

        AuthInfo info = AuthInfoImpl.createFromSubject(subject);
        assertNull(info.getUserID());
        assertTrue(info.getPrincipals().isEmpty());
        assertEquals(0, info.getAttributeNames().length);
    }
}