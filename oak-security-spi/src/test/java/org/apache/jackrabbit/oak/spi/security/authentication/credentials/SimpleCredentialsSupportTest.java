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
package org.apache.jackrabbit.oak.spi.security.authentication.credentials;

import java.util.Map;
import java.util.Set;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SimpleCredentialsSupportTest {

    private final CredentialsSupport credentialsSupport = SimpleCredentialsSupport.getInstance();

    @Test
    public void testGetCredentialClasses() {
        Set<Class> supported = credentialsSupport.getCredentialClasses();

        assertNotNull(supported);
        assertEquals(1, supported.size());
        assertEquals(SimpleCredentials.class, supported.iterator().next());
    }

    @Test
    public void testGetUserId() {
        assertNull(credentialsSupport.getUserId(new TestCredentials()));
        assertNull(credentialsSupport.getUserId(new SimpleCredentials(null, new char[0])));
        assertEquals("uid", credentialsSupport.getUserId(new SimpleCredentials("uid", new char[0])));
    }

    @Test
    public void testGetAttributes() {
        Map<String, ?> attributes = credentialsSupport.getAttributes(new TestCredentials());
        assertNotNull(attributes);
        assertTrue(attributes.isEmpty());

        SimpleCredentials sc = new SimpleCredentials("uid", new char[0]);
        attributes = credentialsSupport.getAttributes(sc);
        assertNotNull(attributes);
        assertTrue(attributes.isEmpty());

        Map<String, ?> expected = ImmutableMap.of("a", "a", "b", Boolean.TRUE, "c", new TestCredentials());
        for (Map.Entry<String, ?> entry : expected.entrySet()) {
            sc.setAttribute(entry.getKey(), entry.getValue());
        }

        attributes = credentialsSupport.getAttributes(sc);
        assertNotNull(attributes);
        assertEquals(3, attributes.size());
        assertEquals(expected, attributes);
    }

    @Test
    public void testSetAttributes() {
        Map<String, ?> attributes = credentialsSupport.getAttributes(new TestCredentials());
        assertNotNull(attributes);
        assertTrue(attributes.isEmpty());

        SimpleCredentials sc = new SimpleCredentials("uid", new char[0]);

        Map<String, ?> expected = ImmutableMap.of("a", "a", "b", Boolean.TRUE, "c", new TestCredentials());
        credentialsSupport.setAttributes(sc, expected);

        for (Map.Entry<String, ?> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), sc.getAttribute(entry.getKey()));
        }

        attributes = credentialsSupport.getAttributes(sc);
        assertNotNull(attributes);
        assertEquals(3, attributes.size());
        assertEquals(expected, attributes);
    }

    @Test
    public void testSetAttributesFalse() {
        assertFalse(credentialsSupport.setAttributes(new Credentials() {}, ImmutableMap.of("a", "value")));
    }

    private static final class TestCredentials implements Credentials {}
}