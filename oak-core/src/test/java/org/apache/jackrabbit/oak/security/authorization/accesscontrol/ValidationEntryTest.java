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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ValidationEntryTest extends AbstractAccessControlTest {

    private ValidationEntry entry;

    @Before
    public void before() throws Exception {
        super.before();
        entry = new ValidationEntry("principalName", PrivilegeBits.BUILT_IN.get(JCR_WRITE), false, ImmutableSet.of(mock(Restriction.class)));
    }

    @Test
    public void testEquals() {
        assertEquals(entry, new ValidationEntry(entry.principalName, entry.privilegeBits, entry.isAllow, entry.restrictions));
        assertEquals(entry, new ValidationEntry(entry.principalName, entry.privilegeBits, entry.isAllow, entry.restrictions, entry.index+1));
        assertTrue(entry.equals(entry));
    }

    @Test
    public void testNotEquals() throws Exception {
        assertNotEquals(entry, new ValidationEntry("other", entry.privilegeBits, entry.isAllow, entry.restrictions));
        assertNotEquals(entry, new ValidationEntry(entry.principalName, PrivilegeBits.BUILT_IN.get(REP_WRITE), entry.isAllow, entry.restrictions));
        assertNotEquals(entry, new ValidationEntry(entry.principalName, entry.privilegeBits, !entry.isAllow, entry.restrictions));
        assertNotEquals(entry, new ValidationEntry(entry.principalName, entry.privilegeBits, entry.isAllow, ImmutableSet.of()));
        assertNotEquals(entry, createEntry(new PrincipalImpl(entry.principalName), entry.privilegeBits, entry.isAllow, entry.restrictions));
    }

    @Test
    public void testHashcode() {
        int hc = entry.hashCode();
        assertEquals(hc, entry.hashCode());
        assertEquals(hc, new ValidationEntry(entry.principalName, entry.privilegeBits, entry.isAllow, entry.restrictions).hashCode());
        assertEquals(hc, new ValidationEntry(entry.principalName, entry.privilegeBits, entry.isAllow, entry.restrictions, entry.index+1).hashCode());

        assertNotEquals(hc, new ValidationEntry("other", entry.privilegeBits, entry.isAllow, entry.restrictions).hashCode());
        assertNotEquals(hc, new ValidationEntry(entry.principalName, PrivilegeBits.BUILT_IN.get(REP_WRITE), entry.isAllow, entry.restrictions).hashCode());
        assertNotEquals(hc, new ValidationEntry(entry.principalName, entry.privilegeBits, !entry.isAllow, entry.restrictions).hashCode());
        assertNotEquals(hc, new ValidationEntry(entry.principalName, entry.privilegeBits, entry.isAllow, ImmutableSet.of()).hashCode());
    }
}