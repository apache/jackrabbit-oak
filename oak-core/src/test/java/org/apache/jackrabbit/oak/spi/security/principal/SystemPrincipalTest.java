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

import org.apache.jackrabbit.api.security.principal.JackrabbitPrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SystemPrincipalTest {

    @Test
    public void testGetName() {
        assertEquals("system", SystemPrincipal.INSTANCE.getName());
    }

    @Test
    public void testEquals() {
        assertEquals(SystemPrincipal.INSTANCE, SystemPrincipal.INSTANCE);
    }

    @Test
    public void testSame() {
        assertSame(SystemPrincipal.INSTANCE, SystemPrincipal.INSTANCE);
    }

    @Test
    public void testHashCode() {
        assertTrue(SystemPrincipal.INSTANCE.hashCode() == SystemPrincipal.INSTANCE.hashCode());
    }

    @Test
    public void testNotEqualsOtherPrincipalWithSameName() {
        Principal another = new Principal() {
            public String getName() {
                return SystemPrincipal.INSTANCE.getName();
            }
        };
        assertFalse(SystemPrincipal.INSTANCE.equals(another));
    }

    @Test
    public void testEqualsOtherJackrabbitPrincipal() {
        Principal another = new OtherSystem();
        assertFalse(SystemPrincipal.INSTANCE.equals(another));
    }

    @Test
    public void testToString() {
        assertNotNull(SystemPrincipal.INSTANCE.toString());
        assertNotEquals(SystemPrincipal.INSTANCE.getName(), SystemPrincipal.INSTANCE.toString());
    }

    //--------------------------------------------------------------------------

    private class OtherSystem implements JackrabbitPrincipal {
        public String getName() {
            return SystemPrincipal.INSTANCE.getName();
        }
        @Override
        public boolean equals(Object o) {
            if (o instanceof JackrabbitPrincipal) {
                return getName().equals(((JackrabbitPrincipal) o).getName());
            }
            return false;
        }
        @Override
        public int hashCode() {
            return getName().hashCode();
        }
    }
}