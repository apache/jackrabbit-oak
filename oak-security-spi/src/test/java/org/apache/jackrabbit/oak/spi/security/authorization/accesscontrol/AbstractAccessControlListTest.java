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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AbstractRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests for functionality provided by {@link AbstractAccessControlList}.
 */
public class AbstractAccessControlListTest extends AbstractAccessControlTest {

    protected String getTestPath() {
        return "/testPath";
    }

    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    protected RestrictionProvider getRestrictionProvider() {
        Map<String, RestrictionDefinition> rDefs = new HashMap();
        rDefs.put("r1", new RestrictionDefinitionImpl("r1", Type.STRING, true));
        rDefs.put("r2", new RestrictionDefinitionImpl("r2", Type.LONGS, false));

        return new AbstractRestrictionProvider(rDefs) {
            @Nonnull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected AbstractAccessControlList createEmptyACL() {
        return createACL(getTestPath(), Collections.<JackrabbitAccessControlEntry>emptyList(), getNamePathMapper());
    }

    protected AbstractAccessControlList createACL(@Nonnull JackrabbitAccessControlEntry... entries) {
        return createACL(getTestPath(), Lists.newArrayList(entries), getNamePathMapper());
    }

    protected AbstractAccessControlList createACL(@Nonnull List<JackrabbitAccessControlEntry> entries) {
        return createACL(getTestPath(), entries, getNamePathMapper());
    }

    protected AbstractAccessControlList createACL(@Nullable String jcrPath,
                                                  @Nonnull ACE... entries) {
        return createACL(jcrPath, Lists.newArrayList(entries), getNamePathMapper());
    }

    protected AbstractAccessControlList createACL(@Nullable String jcrPath,
                                                  @Nonnull List<JackrabbitAccessControlEntry> entries,
                                                  @Nonnull NamePathMapper namePathMapper) {
        return createACL(jcrPath, entries, namePathMapper, getRestrictionProvider());
    }

    protected AbstractAccessControlList createACL(@Nullable String jcrPath,
                                                  @Nonnull List<JackrabbitAccessControlEntry> entries,
                                                  @Nonnull NamePathMapper namePathMapper,
                                                  @Nonnull RestrictionProvider restrictionProvider) {
        return new TestACL(jcrPath, restrictionProvider, namePathMapper, entries);
    }

    protected List<JackrabbitAccessControlEntry> createTestEntries() throws RepositoryException {
        List<JackrabbitAccessControlEntry> entries = new ArrayList(3);
        for (int i = 0; i < 3; i++) {
            entries.add(createEntry(
                    new PrincipalImpl("testPrincipal" + i), PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ), true));
        }
        return entries;
    }

    @Test
    public void testGetNamePathMapper() throws Exception {
        assertSame(getNamePathMapper(), createEmptyACL().getNamePathMapper());
        assertSame(NamePathMapper.DEFAULT, createACL(getTestPath(), ImmutableList.<JackrabbitAccessControlEntry>of(), NamePathMapper.DEFAULT).getNamePathMapper());
    }

    @Test
    public void testGetPath() {
        NamePathMapper npMapper = mockNamePathMapper(getTestPath());

        // map of jcr-path to standard jcr-path
        Map<String, String> paths = new HashMap<String, String>();
        paths.put(null, null);
        paths.put(getTestPath(), getTestPath());
        paths.put("/", "/");
        paths.put("/jr:testPath", "/jr:testPath");
        paths.put("/{http://jackrabbit.apache.org}testPath", "/jr:testPath");

        for (String path : paths.keySet()) {
            AbstractAccessControlList acl = createACL(path, Collections.<JackrabbitAccessControlEntry>emptyList(), npMapper);
            assertEquals(paths.get(path), acl.getPath());
        }
    }

    @Test
    public void testGetOakPath() {
        NamePathMapper npMapper = mockNamePathMapper(getTestPath());

        // map of jcr-path to oak path
        Map<String, String> paths = new HashMap<String, String>();
        paths.put(null, null);
        paths.put(getTestPath(), getTestPath());
        paths.put("/", "/");
        String oakPath = "/oak:testPath";
        paths.put("/jr:testPath", oakPath);
        paths.put("/{http://jackrabbit.apache.org}testPath", oakPath);

        // test if oak-path is properly set.
        for (String path : paths.keySet()) {
            AbstractAccessControlList acl = createACL(path, Collections.<JackrabbitAccessControlEntry>emptyList(), npMapper);
            assertEquals(paths.get(path), acl.getOakPath());
        }
    }

    private static NamePathMapper mockNamePathMapper(String testPath) {
        NamePathMapper npMapper = Mockito.mock(NamePathMapper.class);
        when(npMapper.getOakPath("/")).thenReturn("/");
        when(npMapper.getOakPath(null)).thenReturn(null);
        when(npMapper.getOakPath(testPath)).thenReturn(testPath);
        when(npMapper.getOakPath("/jr:testPath")).thenReturn("/oak:testPath");
        when(npMapper.getOakPath("/{http://jackrabbit.apache.org}testPath")).thenReturn("/oak:testPath");
        when(npMapper.getJcrPath("/")).thenReturn("/");
        when(npMapper.getJcrPath(null)).thenReturn(null);
        when(npMapper.getJcrPath(testPath)).thenReturn(testPath);
        when(npMapper.getJcrPath("/oak:testPath")).thenReturn("/jr:testPath");
        when(npMapper.getJcrPath("/{http://jackrabbit.apache.org}testPath")).thenReturn("/jr:testPath");
        return npMapper;
    }

    @Test
    public void testEmptyAcl() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();

        assertNotNull(acl.getAccessControlEntries());
        assertNotNull(acl.getEntries());

        assertTrue(acl.getAccessControlEntries().length == 0);
        assertEquals(acl.getAccessControlEntries().length, acl.getEntries().size());
        assertEquals(0, acl.size());
        assertTrue(acl.isEmpty());
    }

    @Test
    public void testSize() throws RepositoryException {
        AbstractAccessControlList acl = createACL(createTestEntries());
        assertEquals(3, acl.size());
    }

    @Test
    public void testIsEmpty() throws RepositoryException {
        AbstractAccessControlList acl = createACL(createTestEntries());
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testGetEntries() throws RepositoryException {
        List<JackrabbitAccessControlEntry> aces = createTestEntries();
        AbstractAccessControlList acl = createACL(aces);

        assertNotNull(acl.getEntries());
        assertNotNull(acl.getAccessControlEntries());

        assertEquals(aces.size(), acl.getEntries().size());
        assertEquals(aces.size(), acl.getAccessControlEntries().length);
        assertTrue(acl.getEntries().containsAll(aces));
        assertTrue(Arrays.asList(acl.getAccessControlEntries()).containsAll(aces));
    }

    @Test
    public void testGetRestrictionNames() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();

        String[] restrNames = acl.getRestrictionNames();
        assertNotNull(restrNames);
        List<String> names = Lists.newArrayList(restrNames);
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(getTestPath())) {
            assertTrue(names.remove(getNamePathMapper().getJcrName(def.getName())));
        }
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetRestrictionType() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(getTestPath())) {
            int reqType = acl.getRestrictionType(getNamePathMapper().getJcrName(def.getName()));

            assertTrue(reqType > PropertyType.UNDEFINED);
            assertEquals(def.getRequiredType().tag(), reqType);
        }
    }

    @Test
    public void testGetRestrictionTypeForUnknownName() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();
        // for backwards compatibility getRestrictionType(String) must return
        // UNDEFINED for a unknown restriction name:
        assertEquals(PropertyType.UNDEFINED, acl.getRestrictionType("unknownRestrictionName"));
    }

    @Test
    public void testIsMultiValueRestriction() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(getTestPath())) {
            boolean isMv = acl.isMultiValueRestriction(getNamePathMapper().getJcrName(def.getName()));

            assertEquals(def.getRequiredType().isArray(), isMv);
        }
    }

    @Test
    public void testIsMultiValueRestrictionForUnknownName() throws RepositoryException {
        assertFalse(createEmptyACL().isMultiValueRestriction("unknownRestrictionName"));
    }
}