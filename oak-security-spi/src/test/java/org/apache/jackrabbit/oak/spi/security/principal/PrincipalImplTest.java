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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.api.security.principal.JackrabbitPrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * PrincipalImplTest...
 */
public class PrincipalImplTest {

    private Principal principal = new PrincipalImpl("name");

    @Test
    public void testGetName() {
        assertEquals("name", principal.getName());
    }

    @Test
    public void testEqualsSame() {
        assertTrue(principal.equals(principal));
    }

    @Test
    public void testEquals() {
        List<Principal> principals = new ArrayList<Principal>();
        principals.add(new PrincipalImpl("name"));
        principals.add(new TestPrincipal("name"));
        principals.add(new JackrabbitPrincipal() {
            @Override
            public String getName() {
                return "name";
            }
        });

        for (Principal p : principals) {
            assertEquals(principal, p);
        }
    }

    @Test
    public void testNotEquals() {
        List<Principal> principals = new ArrayList<Principal>();
        principals.add(new PrincipalImpl("otherName"));
        principals.add(new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        });

        for (Principal p : principals) {
            assertFalse(principal.equals(p));
        }
    }

    @Test
    public void testToString() {
        assertNotNull(principal.toString());
        assertTrue(principal.toString().endsWith(principal.getName()));
        assertTrue(principal.toString().startsWith(principal.getClass().getName()));
    }

    //--------------------------------------------------------------------------

    private class TestPrincipal extends PrincipalImpl {

        private TestPrincipal(String name) {
            super(name);
        }
    }
}