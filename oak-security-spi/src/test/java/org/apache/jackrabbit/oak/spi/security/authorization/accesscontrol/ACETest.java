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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Tests for {@link ACE}
 */
public class ACETest extends AbstractAccessControlTest {

    private Value globValue;
    private Value[] nameValues;
    private Value nameValue;

    @Before
    public void before() throws Exception {
        ValueFactory valueFactory = new ValueFactoryImpl(mock(Root.class), getNamePathMapper());
        globValue = valueFactory.createValue("*");
        nameValue = valueFactory.createValue("nt:file", PropertyType.NAME);
        nameValues = new Value[] {
                valueFactory.createValue("nt:folder", PropertyType.NAME),
                valueFactory.createValue("nt:file", PropertyType.NAME)
        };
    }
    
    private ACE mockACE(@NotNull Principal principal, @NotNull PrivilegeBits bits, boolean isAllow, @Nullable Set<Restriction> rest) {
        ACE ace = mock(ACE.class, withSettings().useConstructor(principal, bits, isAllow, rest, getNamePathMapper()).defaultAnswer(CALLS_REAL_METHODS));
        when(ace.getPrivilegeBitsProvider()).thenReturn(getBitsProvider());
        return ace;
    }

    private ACE createEntry(Restriction... restrictions) throws Exception {
        return createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true, restrictions);
    }

    private Restriction createRestriction(String name, String value) {
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
        ACE ace = createEntry(true, JCR_READ);
        assertTrue(ace.isAllow());

        ace = createEntry(false, JCR_READ);
        assertFalse(ace.isAllow());
    }

    @Test
    public void testGetPrincipal() throws RepositoryException {
        ACE tmpl = createEntry(true, JCR_READ);
        assertNotNull(tmpl.getPrincipal());
        assertEquals(testPrincipal.getName(), tmpl.getPrincipal().getName());
        assertSame(testPrincipal, tmpl.getPrincipal());
    }

    @Test(expected = AccessControlException.class)
    public void testNullPrincipal() throws Exception {
        createEntry(null, PrivilegeBits.BUILT_IN.get(JCR_READ), true);
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
        ACE entry = createEntry(true, JCR_READ);

        PrivilegeBits bits = entry.getPrivilegeBits();
        assertNotNull(bits);
        assertEquals(PrivilegeBits.BUILT_IN.get(JCR_READ), bits);

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

    @Test(expected = AccessControlException.class)
    public void testNullPrivileges() throws Exception {
        createEmptyEntry(null);
    }

    @Test(expected = AccessControlException.class)
    public void testEmptyPrivileges() throws Exception {
        createEmptyEntry(PrivilegeBits.EMPTY);
    }

    @NotNull 
    private ACE createEmptyEntry(@Nullable PrivilegeBits bits) throws AccessControlException {
        return new ACE(testPrincipal, bits, true, Collections.emptySet(), getNamePathMapper()) {
            @Override
            protected @NotNull PrivilegeBitsProvider getPrivilegeBitsProvider() {
                return getBitsProvider();
            }

            @Override
            public Privilege[] getPrivileges() {
                return new Privilege[0];
            }
        };
    }
    
    @Test
    public void testGetPrivilegeCollection() throws RepositoryException {
        ACE entry = mockACE(testPrincipal,PrivilegeBits.BUILT_IN.get(JCR_READ), true, null);

        PrivilegeCollection pc = entry.getPrivilegeCollection();
        assertTrue(pc instanceof AbstractPrivilegeCollection);
        
        assertArrayEquals(entry.getPrivileges(), pc.getPrivileges());
        verify(entry, times(2)).getPrivileges();
        
        assertTrue(pc.includes(JCR_READ));
        assertTrue(pc.includes(REP_READ_NODES));
        assertTrue(pc.includes(REP_READ_PROPERTIES));
        assertFalse(pc.includes(JCR_READ, JCR_WRITE));
        
        assertEquals(pc, entry.getPrivilegeCollection());
    }

    @Test
    public void testNullRestrictions() {
        ACE ace = mockACE(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true, null);
        assertTrue(ace.getRestrictions().isEmpty());
    }

    @Test
    public void testRestrictions() {
        Restriction r = new RestrictionImpl(PropertyStates.createProperty("r", "v"), false);
        Restriction r2 = new RestrictionImpl(PropertyStates.createProperty("r2", ImmutableList.of("v"), Type.STRINGS), false);
        Set<Restriction> restrictions = Set.of(r, r2);
        ACE ace = mockACE(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true, restrictions);
        assertFalse(ace.getRestrictions().isEmpty());
        assertNotSame(restrictions, ace.getRestrictions());
        assertTrue(Iterables.elementsEqual(restrictions, ace.getRestrictions()));
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

        Set<Restriction> expected = Set.of(nameRestr, globRestr);
        ACE ace = createEntry(nameRestr, globRestr);

        assertEquals(expected, ace.getRestrictions());
    }

    @Test
    public void testGetRestrictionsNone() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true);

        assertTrue(ace.getRestrictions().isEmpty());
    }

    @Test
    public void testEqualsSameACE() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true);

        assertEquals(ace, ace);
    }

    @Test
    public void testEqualsACE() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true);
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true);

        assertEquals(ace, ace2);
    }

    @Test
    public void testEqualsOtherEntryImpl() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true);

        assertNotEquals(ace, mock(JackrabbitAccessControlEntry.class));
    }

    @Test
    public void testEqualsDifferentAllow() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true);
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), false);

        assertNotEquals(ace, ace2);
    }

    @Test
    public void testEqualsDifferentPrincipal() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true);
        ACE ace2 = createEntry(EveryonePrincipal.getInstance(), PrivilegeBits.BUILT_IN.get(JCR_READ), true);

        assertNotEquals(ace, ace2);
    }

    @Test
    public void testEqualsDifferentPrivs() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true);
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_ADD_CHILD_NODES), true);

        assertNotEquals(ace, ace2);
    }

    @Test
    public void testEqualsDifferentRestrictions() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true, createRestriction("name2", "val"));
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true, createRestriction("name", "val"));

        assertNotEquals(ace, ace2);
    }

    @Test
    public void testEqualsDifferentRestrictionValue() throws Exception {
        ACE ace = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true, createRestriction("name", "val"));
        ACE ace2 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(JCR_READ), true, createRestriction("name", "val2"));

        assertNotEquals(ace, ace2);
    }
}