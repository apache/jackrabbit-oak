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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.authorization.PrincipalSetPolicy;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.security.AccessControlException;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class CugPolicyImplTest extends AbstractSecurityTest {

    private String path = "/testPath";
    private PrincipalManager principalManager;
    private Principal testPrincipal = new PrincipalImpl("test");
    Set<Principal> principals = Set.of(testPrincipal);

    private CugExclude exclude = new CugExclude.Default();

    @Override
    public void before() throws Exception {
        super.before();

        principalManager = getPrincipalManager(root);
    }

    private CugPolicyImpl createEmptyCugPolicy() {
        return createEmptyCugPolicy(ImportBehavior.ABORT);
    }

    private CugPolicyImpl createEmptyCugPolicy(int importBehavior) {
        return new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, importBehavior, exclude);
    }

    private CugPolicyImpl createCugPolicy(@NotNull Iterable<Principal> principals) {
        return createCugPolicy(ImportBehavior.ABORT, principals);
    }

    private CugPolicyImpl createCugPolicy(int importBehavior, @NotNull Iterable<Principal> principals) {
        return new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, importBehavior, exclude, principals, false);
    }

    private Principal getExcludedPrincipal() {
        return (SystemUserPrincipal) () -> "excluded";
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
    public void testCreateWithDuplicateName() {
        Set<Principal> duplication = Set.of(testPrincipal, () -> testPrincipal.getName());
        assertEquals(2, duplication.size());

        CugPolicyImpl cugPolicy = createCugPolicy(duplication);
        assertEquals(1, cugPolicy.getPrincipals().size());
        assertEquals(1, Iterables.size(cugPolicy.getPrincipalNames()));
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
        CugPolicy cug = createEmptyCugPolicy(ImportBehavior.ABORT);
        cug.addPrincipals(
                EveryonePrincipal.getInstance(),
                new PrincipalImpl("unknown"));
    }

    @Test
    public void testAddInvalidPrincipalsBestEffort() throws Exception {
        CugPolicy cug = createCugPolicy(ImportBehavior.BESTEFFORT, principals);
        assertTrue(cug.addPrincipals(
                EveryonePrincipal.getInstance(),
                new PrincipalImpl("unknown")));

        Set<Principal> principalSet = cug.getPrincipals();
        assertEquals(3, principalSet.size());
    }

    @Test
    public void testAddInvalidPrincipalsIgnore() throws Exception {
        CugPolicy cug = createCugPolicy(ImportBehavior.IGNORE, principals);
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
        CugPolicy cug = createCugPolicy(ImportBehavior.BESTEFFORT, principals);
        assertFalse(cug.addPrincipals(
                new PrincipalImpl("test")));

        assertEquals(principals, cug.getPrincipals());
    }

    @Test
    public void testAddContainedPrincipalNonEqualImpl() throws Exception {
        CugPolicy cug = createCugPolicy(ImportBehavior.BESTEFFORT, principals);
        assertFalse(cug.addPrincipals((Principal) () -> testPrincipal.getName()));
        assertEquals(principals, cug.getPrincipals());
    }

    @Test
    public void testAddNullPrincipal() throws Exception {
        CugPolicy cug = createCugPolicy(ImportBehavior.ABORT, principals);
        assertTrue(cug.addPrincipals(EveryonePrincipal.getInstance(), null));

        assertTrue(cug.getPrincipals().contains(EveryonePrincipal.getInstance()));
        assertTrue(cug.getPrincipals().contains(testPrincipal));
    }

    @Test(expected = AccessControlException.class)
    public void testAddEmptyPrincipalName() throws Exception {
        CugPolicy cug = createEmptyCugPolicy(ImportBehavior.BESTEFFORT);
        cug.addPrincipals(new PrincipalImpl(""));
    }

    @Test(expected = AccessControlException.class)
    public void testAddNullPrincipalName() throws Exception {
        CugPolicy cug = createEmptyCugPolicy(ImportBehavior.BESTEFFORT);
        cug.addPrincipals((Principal) () -> null);
    }

    @Test
    public void testRemovePrincipals() throws Exception {
        CugPolicy cug = createCugPolicy(ImportBehavior.BESTEFFORT, Set.of(testPrincipal, EveryonePrincipal.getInstance()));

        assertFalse(cug.removePrincipals(new PrincipalImpl("unknown")));
        assertTrue(cug.removePrincipals(testPrincipal, EveryonePrincipal.getInstance(), new PrincipalImpl("unknown")));
        assertTrue(cug.getPrincipals().isEmpty());
    }

    @Test
    public void testRemoveNullPrincipal() throws Exception {
        CugPolicy cug = createCugPolicy(ImportBehavior.ABORT, principals);
        assertTrue(cug.removePrincipals(testPrincipal, null));
        assertTrue(cug.getPrincipals().isEmpty());
    }

    @Test
    public void testRemoveContainedPrincipalNotEqual() throws Exception {
        CugPolicy cug = createCugPolicy(ImportBehavior.BESTEFFORT, principals);
        assertTrue(cug.removePrincipals((Principal) () -> testPrincipal.getName()));
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
        NamePathMapper mapper = new NamePathMapperImpl(new LocalNameMapper(root, Map.of("quercus", "http://jackrabbit.apache.org/oak/ns/1.0")));

        CugPolicy empty = new CugPolicyImpl(oakPath, mapper, principalManager, ImportBehavior.ABORT, exclude);
        assertEquals("/quercus:testPath", empty.getPath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidImportBehavior() {
        createCugPolicy(-1, principals);
    }

    @Test
    public void testAddSingleExcludedPrincipal() throws Exception {
        CugPolicy cug = createEmptyCugPolicy(ImportBehavior.ABORT);

        assertFalse(cug.addPrincipals(getExcludedPrincipal()));
    }

    @Test
    public void testAddExcludedPrincipal() throws Exception {
        CugPolicyImpl cug = createEmptyCugPolicy(ImportBehavior.ABORT);

        Principal excluded = getExcludedPrincipal();
        assertTrue(cug.addPrincipals(EveryonePrincipal.getInstance(), excluded));
        assertFalse(Iterables.contains(cug.getPrincipalNames(), excluded.getName()));
    }

    @Test
    public void testExcludedPrincipalAddedBefore() {
        Principal excluded = getExcludedPrincipal();
        CugPolicyImpl cug = createCugPolicy(ImportBehavior.ABORT, Collections.singleton(excluded));
        assertTrue(Iterables.contains(cug.getPrincipalNames(), excluded.getName()));
    }

    @Test
    public void removeExcludedPrincipal() throws Exception {
        Principal excluded = getExcludedPrincipal();
        CugPolicyImpl cug = createCugPolicy(ImportBehavior.ABORT, Collections.singleton(excluded));
        assertTrue(cug.removePrincipals(excluded));
    }

    @Test(expected = AccessControlException.class)
    public void testImmutableAddPrincipals() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.ABORT, exclude, principals, true);
        cug.addPrincipals(EveryonePrincipal.getInstance());
    }

    @Test(expected = AccessControlException.class)
    public void testImmutableRemovePrincipals() throws Exception {
        CugPolicy cug = new CugPolicyImpl(path, NamePathMapper.DEFAULT, principalManager, ImportBehavior.ABORT, exclude, principals, true);
        cug.removePrincipals(EveryonePrincipal.getInstance());
    }
}
