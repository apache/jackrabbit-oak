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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static com.google.common.collect.Sets.newHashSet;

public class CompositeCredentialsSupportTest {

    private final TestCredentialsSupport tcs = new TestCredentialsSupport();

    private final CredentialsSupport credentialsSupport = CompositeCredentialsSupport
            .newInstance(() -> newHashSet(SimpleCredentialsSupport.getInstance(), tcs));

    @Test
    public void testGetCredentialClasses() {
        Set<Class> supported = credentialsSupport.getCredentialClasses();
        assertNotNull(supported);
        assertEquals(2, supported.size());
        assertTrue(supported.contains(TestCredentials.class));
        assertTrue(supported.contains(SimpleCredentials.class));
    }

    @Test
    public void testGetUserId() {
        assertEquals("Test1CredentialsSupport", credentialsSupport.getUserId(new TestCredentials()));
        assertNull(credentialsSupport.getUserId(new SimpleCredentials(null, new char[0])));
        assertEquals("uid", credentialsSupport.getUserId(new SimpleCredentials("uid", new char[0])));
        assertNull(credentialsSupport.getUserId(new Credentials() {
        }));
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
        SimpleCredentials sc = new SimpleCredentials("uid", new char[0]);
        TestCredentials tc = new TestCredentials();
        Credentials dummy = new Credentials() {
        };

        Map<String, ?> attributesS = credentialsSupport.getAttributes(sc);
        assertNotNull(attributesS);
        assertTrue(attributesS.isEmpty());

        Map<String, ?> attributesT = credentialsSupport.getAttributes(tc);
        assertNotNull(attributesT);
        assertTrue(attributesT.isEmpty());

        Map<String, ?> attributesD = credentialsSupport.getAttributes(dummy);
        assertNotNull(attributesD);
        assertTrue(attributesD.isEmpty());

        Map<String, ?> expectedS = ImmutableMap.of("a", "a", "b", Boolean.TRUE, "c", new TestCredentials());
        assertTrue(credentialsSupport.setAttributes(sc, expectedS));

        Map<String, ?> expectedT = ImmutableMap.of("test", "Test1CredentialsSupport");
        assertTrue(credentialsSupport.setAttributes(tc, expectedT));

        assertFalse(credentialsSupport.setAttributes(dummy, ImmutableMap.of("none", "none")));

        attributesS = credentialsSupport.getAttributes(sc);
        for (Map.Entry<String, ?> entry : expectedS.entrySet()) {
            assertEquals(entry.getValue(), attributesS.get(entry.getKey()));
        }
        attributesT = credentialsSupport.getAttributes(tc);
        for (Map.Entry<String, ?> entry : expectedT.entrySet()) {
            assertEquals(entry.getValue(), attributesT.get(entry.getKey()));
        }
        attributesD = credentialsSupport.getAttributes(dummy);
        assertNotNull(attributesD);
        assertTrue(attributesD.isEmpty());
    }

    @Test
    public void testEmpty() {
        CredentialsSupport cs = CompositeCredentialsSupport.newInstance(() -> newHashSet());

        assertTrue(cs.getCredentialClasses().isEmpty());
        assertTrue(cs.getAttributes(new TestCredentials()).isEmpty());
    }

    @Test
    public void testSingleValued() {
        CredentialsSupport cs = CompositeCredentialsSupport.newInstance(() -> newHashSet(SimpleCredentialsSupport.getInstance()));

        assertEquals(SimpleCredentialsSupport.getInstance().getCredentialClasses(), cs.getCredentialClasses());
        assertTrue(cs.getAttributes(new TestCredentials()).isEmpty());

        SimpleCredentials creds = new SimpleCredentials("userid", new char[0]);
        creds.setAttribute("attr", "value");
        assertEquals(SimpleCredentialsSupport.getInstance().getAttributes(creds), cs.getAttributes(creds));
    }

    private static final class TestCredentials implements Credentials {
    }

    private static final class TestCredentialsSupport implements CredentialsSupport {

        private final Map<String, Object> attributes = new HashMap<String, Object>();

        @Override
        public Set<Class> getCredentialClasses() {
            return ImmutableSet.of(TestCredentials.class);
        }

        @Override
        public String getUserId(Credentials credentials) {
            if (credentials instanceof TestCredentials) {
                return "Test1CredentialsSupport";
            } else {
                return null;
            }
        }

        @Override
        public Map<String, ?> getAttributes(Credentials credentials) {
            if (credentials instanceof TestCredentials) {
                return attributes;
            } else {
                return ImmutableMap.of();
            }
        }

        @Override
        public boolean setAttributes(Credentials credentials, Map<String, ?> attributes) {
            if (credentials instanceof TestCredentials) {
                this.attributes.putAll(attributes);
                return true;
            } else {
                return false;
            }
        }
    }

}