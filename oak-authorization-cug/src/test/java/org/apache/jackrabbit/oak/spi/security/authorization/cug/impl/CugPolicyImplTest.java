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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.authorization.PrincipalSetPolicy;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class CugPolicyImplTest extends AbstractSecurityTest {

    private String path = "/testPath";
    private PrincipalManager principalManager;
    private Principal testPrincipal = new PrincipalImpl("test");
    Set<Principal> principals = ImmutableSet.of(testPrincipal);

    @Override
    public void before() throws Exception {
        super.before();

        principalManager = getPrincipalManager(root);
    }

    private CugPolicyImpl createEmptyCugPolicy() {
        return new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.ABORT);
    }

    private CugPolicyImpl createCugPolicy(@Nonnull Set<Principal> principals) {
        return new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.ABORT, principals);
    }

    @Test
    public void testPrincipalSetPolicy() {
        assertTrue(createCugPolicy(principals) instanceof PrincipalSetPolicy);
    }

    @Test
    public void testGetPrincipals() {
        CugPolicyImpl cug = createCugPolicy(principals);

        Set<Principal> principalSet = cug.getPrincipals();
        assertFalse(principalSet.isEmpty());
        assertEquals(principals, principalSet);
        assertNotSame(principals, principalSet);
    }

    @Test
    public void testGetPrincipals2() {
        CugPolicyImpl empty = createEmptyCugPolicy();

        assertTrue(empty.getPrincipals().isEmpty());
    }

    @Test
    public void testGetPrincipalNames() {
        CugPolicyImpl cug = createCugPolicy(principals);

        Iterator<String> it = cug.getPrincipalNames().iterator();
        assertTrue(it.hasNext());
        assertEquals("test", it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetPrincipalNames2() {
        CugPolicyImpl empty = createEmptyCugPolicy();

        assertFalse(empty.getPrincipalNames().iterator().hasNext());
    }

    @Test
    public void testAddPrincipals() throws Exception {
        CugPolicy empty = createEmptyCugPolicy();
        assertTrue(empty.addPrincipals(EveryonePrincipal.getInstance()));
        assertFalse(empty.addPrincipals(EveryonePrincipal.getInstance()));

        CugPolicy cug = createCugPolicy(principals);
        assertTrue(cug.addPrincipals(EveryonePrincipal.getInstance()));
        assertFalse(cug.addPrincipals(EveryonePrincipal.getInstance()));
    }

    @Test(expected = AccessControlException.class)
    public void testAddInvalidPrincipalsAbort() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.ABORT);
        cug.addPrincipals(
                EveryonePrincipal.getInstance(),
                new PrincipalImpl("unknown"));
    }

    @Test
    public void testAddInvalidPrincipalsBestEffort() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.BESTEFFORT, principals);
        assertTrue(cug.addPrincipals(
                EveryonePrincipal.getInstance(),
                new PrincipalImpl("unknown")));

        Set<Principal> principalSet = cug.getPrincipals();
        assertEquals(3, principalSet.size());
    }

    @Test
    public void testAddInvalidPrincipalsIgnore() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.IGNORE, principals);
        assertTrue(cug.addPrincipals(
                new PrincipalImpl("unknown"),
                EveryonePrincipal.getInstance()));

        Set<Principal> principalSet = cug.getPrincipals();
        assertEquals(2, principalSet.size());
        assertFalse(principalSet.contains(new PrincipalImpl("unknown")));
        assertFalse(principalSet.contains(new PrincipalImpl("")));
    }

    @Test
    public void testAddContainedPrincipal() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.BESTEFFORT, principals);
        assertFalse(cug.addPrincipals(
                new PrincipalImpl("test")));

        assertEquals(principals, cug.getPrincipals());
    }

    @Test
    public void testAddNullPrincipal() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.ABORT, principals);
        assertTrue(cug.addPrincipals(EveryonePrincipal.getInstance(), null));

        assertTrue(cug.getPrincipals().contains(EveryonePrincipal.getInstance()));
        assertTrue(cug.getPrincipals().contains(testPrincipal));
    }

    @Test(expected = AccessControlException.class)
    public void testAddEmptyPrincipalName() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.BESTEFFORT);
        cug.addPrincipals(new PrincipalImpl(""));
    }

    @Test(expected = AccessControlException.class)
    public void testAddNullPrincipalName() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.BESTEFFORT);
        cug.addPrincipals(new Principal() {
            @Override
            public String getName() {
                return null;
            }
        });
    }

    @Test
    public void testRemovePrincipals() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager,
                ImportBehavior.BESTEFFORT,
                ImmutableSet.of(testPrincipal, EveryonePrincipal.getInstance()));

        assertFalse(cug.removePrincipals(new PrincipalImpl("unknown")));
        assertTrue(cug.removePrincipals(testPrincipal, EveryonePrincipal.getInstance(), new PrincipalImpl("unknown")));
        assertTrue(cug.getPrincipals().isEmpty());
    }

    @Test
    public void testRemoveNullPrincipal() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.ABORT, principals);
        assertTrue(cug.removePrincipals(testPrincipal, null));

        assertTrue(cug.getPrincipals().isEmpty());
    }

    @Test
    public void testGetPath() {
        CugPolicy empty = createEmptyCugPolicy();
        assertEquals(path, empty.getPath());
    }

    @Test
    public void testGetPathWithRemapping() {
        String oakPath = "/oak:testPath";
        NamePathMapper mapper = new NamePathMapperImpl(new LocalNameMapper(root, ImmutableMap.of("quercus", "http://jackrabbit.apache.org/oak/ns/1.0")));

        CugPolicy empty = new CugPolicyImpl(oakPath, mapper, principalManager, ImportBehavior.ABORT);
        assertEquals("/quercus:testPath", empty.getPath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidImportBehavior() {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, -1, principals);
    }
}