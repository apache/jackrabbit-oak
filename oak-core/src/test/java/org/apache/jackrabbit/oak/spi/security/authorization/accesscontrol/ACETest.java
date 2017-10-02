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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import java.util.Set;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link ACE}
 */
public class ACETest extends AbstractAccessControlTest {

    private Value globValue;
    private Value[] nameValues;
    private Value nameValue;

    @Before
    public void before() throws Exception {
        ValueFactory valueFactory = new ValueFactoryImpl(Mockito.mock(Root.class), getNamePathMapper());
        globValue = valueFactory.createValue("*");
        nameValue = valueFactory.createValue("nt:file", PropertyType.NAME);
        nameValues = new Value[] {
                valueFactory.createValue("nt:folder", PropertyType.NAME),
                valueFactory.createValue("nt:file", PropertyType.NAME)
        };
    }

    private ACE createEntry(Restriction... restrictions) throws Exception {
        return createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true, restrictions);
    }

    private Restriction createRestriction(String name, String value) throws Exception {
        return new RestrictionImpl(PropertyStates.createProperty(name, value), false);
    }

    private Restriction createRestriction(String name, Value value) throws Exception {
        return new RestrictionImpl(PropertyStates.createProperty(name, value), false);
    }

    private Restriction createRestriction(String name, Value[] values) throws Exception {
        return new RestrictionImpl(PropertyStates.createProperty(name, ImmutableList.copyOf(values)), false);
    }

    @Test
    public void testIsAllow() throws RepositoryException {
        ACE ace = createEntry(true, PrivilegeConstants.JCR_READ);
        assertTrue(ace.isAllow());

        ace = createEntry(false, PrivilegeConstants.JCR_READ);
        assertFalse(ace.isAllow());
    }

    @Test
    public void testGetPrincipal() throws RepositoryException {
        ACE tmpl = createEntry(true, PrivilegeConstants.JCR_READ);
        assertNotNull(tmpl.getPrincipal());
        assertEquals(testPrincipal.getName(), tmpl.getPrincipal().getName());
        assertSame(testPrincipal, tmpl.getPrincipal());
    }

    @Test(expected = AccessControlException.class)
    public void testNullPrincipal() throws Exception {
        createEntry(null, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);
    }

    @Test(expected = AccessControlException.class)
    public void testNullPrivilegeBits() throws Exception {
        createEntry(testPrincipal, (PrivilegeBits) null, true);
    }

    @Test(expected = AccessControlException.class)
    public void testEmptyPrivilegeBits() throws Exception {
        createEntry(testPrincipal, PrivilegeBits.EMPTY, true);
    }

    @Test
    public void testGetPrivilegeBits() throws RepositoryException {
        ACE entry = createEntry(true, PrivilegeConstants.JCR_READ);

        PrivilegeBits bits = entry.getPrivilegeBits();
        assertNotNull(bits);
        assertEquals(PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), bits);

        entry = createEntry(true, PrivilegeConstants.REP_WRITE);
        bits = entry.getPrivilegeBits();
        assertNotNull(bits);
        assertEquals(PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_WRITE), bits);

        entry = createEntry(true, PrivilegeConstants.JCR_ADD_CHILD_NODES,
                PrivilegeConstants.JCR_REMOVE_CHILD_NODES);
        bits = entry.getPrivilegeBits();
        assertNotNull(bits);

        PrivilegeBits expected = getBitsProvider().getBits(
                PrivilegeConstants.JCR_ADD_CHILD_NODES,
                PrivilegeConstants.JCR_REMOVE_CHILD_NODES);
        assertEquals(expected, bits);
    }

    @Test
    public void testNullPrivileges() throws Exception {
        try {
            new EmptyACE(null);
            fail("Privileges must not be null");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testEmptyPrivileges() throws Exception {
        try {
            new EmptyACE(PrivilegeBits.EMPTY);
            fail("Privileges must not be empty.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetRestrictionNames() throws Exception {
        // empty restrictions
        String[] restrictionNames = createEntry().getRestrictionNames();
        assertNotNull(restrictionNames);
        assertEquals(0, restrictionNames.length);

        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);

        // single restriction
        restrictionNames = createEntry(globRestr).getRestrictionNames();
        assertEquals(1, restrictionNames.length);

        // 2 restrictions
        restrictionNames = createEntry(globRestr, nameRestr).getRestrictionNames();
        assertEquals(2, restrictionNames.length);
    }

    @Test
    public void testGetRestrictionForEmpty() throws Exception {
        // empty restrictions
        Value val = createEntry().getRestriction(AccessControlConstants.REP_GLOB);
        assertNull(val);
    }

    @Test
    public void testGetNonExistingRestriction() throws Exception {
        // single valued restriction
        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);
        ACE ace = createEntry(globRestr);
        assertNull(ace.getRestriction(AccessControlConstants.REP_NT_NAMES));
    }

    @Test
    public void testGetRestrictionForSingleValued() throws Exception {
        // single valued restriction
        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);
        ACE ace = createEntry(globRestr);
        Value val = ace.getRestriction(AccessControlConstants.REP_GLOB);
        assertNotNull(val);
        assertEquals(globValue, val);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test(expected = ValueFormatException.class)
    public void testGetRestrictionForMultiValued() throws Exception {
        // multivalued restriction
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);
        ACE ace = createEntry(nameRestr);
        ace.getRestriction(AccessControlConstants.REP_NT_NAMES);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetRestrictionForMultiValued2() throws Exception {
        // single value restriction stored in multi-value property
        Restriction singleNameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, new Value[] {nameValue});

        ACE ace = createEntry(singleNameRestr);
        Value val = ace.getRestriction(AccessControlConstants.REP_NT_NAMES);
        assertEquals(nameValue, val);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetEmptyRestrictions() throws Exception {
        // empty restrictions
        Value[] vs = createEntry().getRestrictions(AccessControlConstants.REP_GLOB);
        assertNull(vs);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetNonExistingRestrictions() throws Exception {
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);
        ACE ace = createEntry(nameRestr);
        assertNull(ace.getRestrictions(AccessControlConstants.REP_GLOB));
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetRestrictionsForSingleValue() throws Exception {
        // single valued restriction
        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);
        ACE ace = createEntry(globRestr);
        Value[] vs = ace.getRestrictions(AccessControlConstants.REP_GLOB);
        assertNotNull(vs);
        assertArrayEquals(new Value[] {globValue}, vs);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetRestrictionsForMultiValued() throws Exception {
        // multivalued restriction
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);
        ACE ace = createEntry(nameRestr);
        Value[] vs = ace.getRestrictions(AccessControlConstants.REP_NT_NAMES);
        assertEquals(2, vs.length);
        assertArrayEquals(nameValues, vs);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetRestrictionsForMultiValued2() throws Exception {
        // single value restriction stored in multi-value property
        Restriction singleNameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, new Value[]{nameValue});
        ACE ace = createEntry(singleNameRestr);
        Value[] vs = ace.getRestrictions(AccessControlConstants.REP_NT_NAMES);
        assertEquals(1, vs.length);
        assertEquals(nameValue, vs[0]);
    }

    @Test
    public void testGetRestrictions() throws Exception {
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);
        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);

        Set<Restriction> expected = ImmutableSet.of(nameRestr, globRestr);
        ACE ace = createEntry(nameRestr, globRestr);

        assertEquals(expected, ace.getRestrictions());
    }

    @Test
    public void testGetRestrictionsNone() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);

        assertTrue(ace.getRestrictions().isEmpty());
    }

    @Test
    public void testEqualsSameACE() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);

        assertTrue(ace.equals(ace));
    }

    @Test
    public void testEqualsACE() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);

        assertTrue(ace.equals(ace2));
    }

    @Test
    public void testEqualsOtherEntryImpl() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);

        assertFalse(ace.equals(Mockito.mock(JackrabbitAccessControlEntry.class)));
    }

    @Test
    public void testEqualsDifferentAllow() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), false);

        assertFalse(ace.equals(ace2));
    }

    @Test
    public void testEqualsDifferentPrincipal() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);
        ACE ace2 = createEntry(EveryonePrincipal.getInstance(), PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);

        assertFalse(ace.equals(ace2));
    }

    @Test
    public void testEqualsDifferentPrivs() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true);
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_ADD_CHILD_NODES), true);

        assertFalse(ace.equals(ace2));
    }

    @Test
    public void testEqualsDifferentRestrictions() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true, createRestriction("name2", "val"));
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true, createRestriction("name", "val"));

        assertFalse(ace.equals(ace2));
    }

    @Test
    public void testEqualsDifferentRestrictionValue() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true, createRestriction("name", "val"));
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true, createRestriction("name", "val2"));

        assertFalse(ace.equals(ace2));
    }

    private class EmptyACE extends ACE {

        public EmptyACE(PrivilegeBits privilegeBits) throws AccessControlException {
            super(testPrincipal, privilegeBits, true, null, getNamePathMapper());
        }

        @Override
        public Privilege[] getPrivileges() {
            return new Privilege[0];
        }
    }
}