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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
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
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NT_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AbstractEntryTest extends AbstractPrincipalBasedTest {

    private PrivilegeBitsProvider bitsProvider;

    private AbstractEntry entryA;
    private AbstractEntry entryB;

    private Restriction restriction;

    @Before
    public void before() throws Exception {
        super.before();

        this.bitsProvider = new PrivilegeBitsProvider(root);

        ValueFactory vf = getValueFactory(root);
        RestrictionProvider rp = new RestrictionProviderImpl();
        Restriction r = rp.createRestriction(TEST_OAK_PATH, REP_NT_NAMES, vf.createValue(getNamePathMapper().getJcrName(NT_OAK_UNSTRUCTURED), PropertyType.NAME));

        Principal principal = getTestSystemUser().getPrincipal();
        entryA = new TestEntry(TEST_OAK_PATH, principal, bitsProvider.getBits(PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT, PrivilegeConstants.REP_WRITE), r);
        entryB = new TestEntry(null, principal, bitsProvider.getBits(PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT));

        restriction = rp.createRestriction(entryA.getOakPath(), REP_GLOB, vf.createValue("*"));
    }

    @Test
    public void testHashCode() throws Exception {
        assertNotEquals(entryA.hashCode(), entryB.hashCode());

        // same entry -> same hash
        assertEquals(entryA.hashCode(), entryA.hashCode());

        // equivalent entry -> same hash
        assertEquals(entryA.hashCode(), new TestEntry(entryA).hashCode());
        assertEquals(entryB.hashCode(), new TestEntry(entryB).hashCode());

        // different restrictions -> different hash
        AbstractEntry differentRestriction = new TestEntry(entryA.getOakPath(), entryA.getPrincipal(), entryA.getPrivilegeBits(), restriction);
        assertNotEquals(entryA.hashCode(), differentRestriction.hashCode());

        // different path -> different hash
        AbstractEntry differentPath = new TestEntry(PathUtils.ROOT_PATH, entryA.getPrincipal(), entryA.getPrivilegeBits(), entryA.getRestrictions().toArray(new Restriction[0]));
        assertNotEquals(entryA.hashCode(), differentPath.hashCode());

        // different principal -> different hash
        AbstractEntry differentPrincipal = new TestEntry(entryB.getOakPath(), EveryonePrincipal.getInstance(), entryB.getPrivilegeBits(), entryB.getRestrictions().toArray(new Restriction[0]));
        assertNotEquals(entryB.hashCode(), differentPrincipal.hashCode());

        // different path -> different hash
        AbstractEntry differentPrivs = new TestEntry(entryB.getOakPath(), entryB.getPrincipal(), bitsProvider.getBits(PrivilegeConstants.JCR_READ), entryB.getRestrictions().toArray(new Restriction[0]));
        assertNotEquals(entryB.hashCode(), differentPrivs.hashCode());
    }

    @Test
    public void testEquals() throws Exception {
        assertNotEquals(entryA, entryB);
        assertNotEquals(entryB, entryA);

        assertEquals(entryA, entryA);

        // equivalent entry -> equals
        assertEquals(entryA, new TestEntry(entryA));
        assertEquals(entryB, new TestEntry(entryB));

        // different restrictions -> different hash
        AbstractEntry differentRestriction = new TestEntry(entryA.getOakPath(), entryA.getPrincipal(), entryA.getPrivilegeBits(), restriction);
        assertNotEquals(entryA, differentRestriction);

        // different path -> different hash
        AbstractEntry differentPath = new TestEntry(PathUtils.ROOT_PATH, entryA.getPrincipal(), entryA.getPrivilegeBits(), entryA.getRestrictions().toArray(new Restriction[0]));
        assertNotEquals(entryA, differentPath);

        // different principal -> different hash
        AbstractEntry differentPrincipal = new TestEntry(entryB.getOakPath(), EveryonePrincipal.getInstance(), entryB.getPrivilegeBits(), entryB.getRestrictions().toArray(new Restriction[0]));
        assertNotEquals(entryB, differentPrincipal);

        // different path -> different hash
        AbstractEntry differentPrivs = new TestEntry(entryB.getOakPath(), entryB.getPrincipal(), bitsProvider.getBits(PrivilegeConstants.JCR_READ), entryB.getRestrictions().toArray(new Restriction[0]));
        assertNotEquals(entryB, differentPrivs);
    }

    private final class TestEntry extends AbstractEntry {

        TestEntry(@Nullable String oakPath, @NotNull Principal principal, @NotNull PrivilegeBits privilegeBits, @NotNull Restriction... restrictions) throws AccessControlException {
            super(oakPath, principal, privilegeBits, Set.of(restrictions), AbstractEntryTest.this.getNamePathMapper());
        }

        TestEntry(@NotNull AbstractEntry base) throws AccessControlException {
            super(base.getOakPath(), base.getPrincipal(), base.getPrivilegeBits(), base.getRestrictions(), AbstractEntryTest.this.getNamePathMapper());
        }

        @Override
        @NotNull NamePathMapper getNamePathMapper() {
            return AbstractEntryTest.this.getNamePathMapper();
        }

        @Override
        protected @NotNull PrivilegeBitsProvider getPrivilegeBitsProvider() {
            return bitsProvider;
        }
        
        @Override
        public Privilege[] getPrivileges() {
            try {
                return privilegesFromNames(bitsProvider.getPrivilegeNames(getPrivilegeBits()));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }
    }
}