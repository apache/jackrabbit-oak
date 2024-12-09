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
package org.apache.jackrabbit.oak.spi.security.authorization.restriction;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CompositeRestrictionProviderTest implements AccessControlConstants {

    private static final String NAME_LONGS = "longs";
    private static final String NAME_BOOLEAN = "boolean";

    private static final Restriction GLOB_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(REP_GLOB, "*"), false);
    private static final Restriction NT_PREFIXES_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(REP_PREFIXES, ImmutableList.of(), Type.STRINGS), false);
    private static final Restriction MANDATORY_BOOLEAN_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(NAME_BOOLEAN, true, Type.BOOLEAN), true);
    private static final Restriction LONGS_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(NAME_LONGS, ImmutableList.of(Long.MAX_VALUE), Type.LONGS), false);
    private static final Restriction UNKNOWN_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty("unknown", "string"), false);

    private final AbstractRestrictionProvider rp1 = spy(createRestrictionProvider(GLOB_RESTRICTION.getDefinition(), NT_PREFIXES_RESTRICTION.getDefinition()));
    private final AbstractRestrictionProvider rp2 = spy(createRestrictionProvider(MANDATORY_BOOLEAN_RESTRICTION.getDefinition(), LONGS_RESTRICTION.getDefinition()));

    private final Set<String> supported = Set.of(
            MANDATORY_BOOLEAN_RESTRICTION.getDefinition().getName(),
            LONGS_RESTRICTION.getDefinition().getName(),
            REP_PREFIXES,
            REP_GLOB);
    private final RestrictionProvider provider = CompositeRestrictionProvider.newInstance(rp1, rp2);

    private final ValueFactory vf = new ValueFactoryImpl(mock(Root.class), NamePathMapper.DEFAULT);

    @Before
    public void before() {
        verify(rp1).setComposite(((CompositeRestrictionProvider) provider));
        verify(rp2).setComposite(((CompositeRestrictionProvider) provider));
        reset(rp1, rp2);
    }
    
    @NotNull
    private AbstractRestrictionProvider createRestrictionProvider(@NotNull RestrictionDefinition... supportedDefinitions) {
        return createRestrictionProvider(null, null, supportedDefinitions);
    }

    @NotNull
    private AbstractRestrictionProvider createRestrictionProvider(@Nullable RestrictionPattern pattern, @Nullable Restriction toRead, @NotNull RestrictionDefinition... supportedDefinitions) {
        Map<String, RestrictionDefinition> builder = new HashMap<>();
        for (RestrictionDefinition def : supportedDefinitions) {
            builder.put(def.getName(), def);
        }
        return new AbstractRestrictionProvider(Collections.unmodifiableMap(builder)) {
            @Override
            public @NotNull Set<Restriction> readRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) {
                if (toRead != null) {
                    return Set.of(toRead);
                } else {
                    return super.readRestrictions(oakPath, aceTree);
                }
            }

            @NotNull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
                return getPattern();
            }

            @NotNull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
                return getPattern();
            }

            private @NotNull RestrictionPattern getPattern() {
                if (pattern == null) {
                    throw new UnsupportedOperationException();
                } else {
                    return pattern;
                }
            }
        };
    }

    private Tree getAceTree(Restriction... restrictions) {
        Tree restrictionsTree = mock(Tree.class);
        when(restrictionsTree.getName()).thenReturn(REP_RESTRICTIONS);
        when(restrictionsTree.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_RESTRICTIONS, Type.NAME));
        List<PropertyState> properties = new ArrayList<>();
        for (Restriction r : restrictions) {
            String name = r.getDefinition().getName();
            when(restrictionsTree.getProperty(name)).thenReturn(r.getProperty());
            when(restrictionsTree.hasProperty(name)).thenReturn(true);
            properties.add(r.getProperty());
        }
        when(restrictionsTree.getProperties()).thenReturn((Iterable)properties);
        when(restrictionsTree.exists()).thenReturn(true);

        Tree ace = mock(Tree.class);
        when(ace.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_GRANT_ACE, Type.NAME));
        when(ace.getChild(REP_RESTRICTIONS)).thenReturn(restrictionsTree);
        when(ace.exists()).thenReturn(true);

        return ace;
    }

    @Test
    public void testEmpty() {
        assertSame(RestrictionProvider.EMPTY, CompositeRestrictionProvider.newInstance(Collections.emptySet()));
    }

    @Test
    public void testSingle() {
        assertSame(rp1, CompositeRestrictionProvider.newInstance(Collections.singleton(rp1)));
    }

    @Test
    public void testNewInstance() {
        RestrictionProvider crp = CompositeRestrictionProvider.newInstance(Set.of(rp1, rp2));
        RestrictionProvider crp2 = CompositeRestrictionProvider.newInstance(rp1, rp2);

        assertEquals(crp.getSupportedRestrictions("/testPath"), crp2.getSupportedRestrictions("/testPath"));
    }

    @Test
    public void testGetSupportedRestrictions() {
        String[] paths = new String[] {null, "/testPath"};
        for (String path : paths) {
            Set<RestrictionDefinition> defs = provider.getSupportedRestrictions(path);
            int expectedSize = rp1.getSupportedRestrictions(path).size() + rp2.getSupportedRestrictions(path).size();
            assertEquals(expectedSize, defs.size());
            assertTrue(defs.containsAll(rp1.getSupportedRestrictions(path)));
            assertTrue(defs.containsAll(rp2.getSupportedRestrictions(path)));
        }
    }

    @Test
    public void testCreateRestriction() throws Exception {
        Map<String, Value> valid = Map.of(
                NAME_BOOLEAN, vf.createValue(true),
                NAME_LONGS, vf.createValue(10),
                REP_GLOB, vf.createValue("*")
        );
        for (Map.Entry<String, Value> entry : valid.entrySet()) {
            provider.createRestriction("/testPath", entry.getKey(), entry.getValue());
        }
        verify(rp1, times(1)).createRestriction(anyString(), anyString(), any(Value.class));
        verify(rp2, times(2)).createRestriction(anyString(), anyString(), any(Value.class));
    }

    @Test(expected = AccessControlException.class)
    public void testCreateRestrictionWithInvalidPath() throws Exception {
        provider.createRestriction(null, REP_GLOB, vf.createValue("*"));
    }

    @Test
    public void testCreateInvalidRestriction() throws Exception {
        Map<String, Value> invalid = Map.of(
                NAME_BOOLEAN, vf.createValue("wrong_type"),
                REP_GLOB, vf.createValue(true)
        );
        for (Map.Entry<String, Value> entry : invalid.entrySet()) {
            String name = entry.getKey();
            try {
                provider.createRestriction("/testPath", name, entry.getValue());
                fail("invalid restriction " + name);
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test
    public void testMvCreateRestriction() throws RepositoryException {
        Map<String, Value[]> valid = Map.of(
                NAME_LONGS, new Value[] {vf.createValue(100)},
                REP_PREFIXES, new Value[] {vf.createValue("prefix"), vf.createValue("prefix2")}
        );
        for (Map.Entry<String, Value[]> entry : valid.entrySet()) {
            provider.createRestriction("/testPath", entry.getKey(), entry.getValue());
        }
        verify(rp1, times(1)).createRestriction("/testPath", REP_PREFIXES, valid.get(REP_PREFIXES));
        verify(rp2, times(1)).createRestriction("/testPath", NAME_LONGS, valid.get(NAME_LONGS));
    }

    @Test(expected = AccessControlException.class)
    public void testCreateMvRestrictionWithInvalidPath() throws Exception {
        provider.createRestriction(null, REP_PREFIXES, new Value[] {vf.createValue("jcr")});
    }

    @Test
    public void testCreateInvalidMvRestriction() throws Exception {
        Map<String, Value[]> invalid = Map.of(
                NAME_BOOLEAN, new Value[] {vf.createValue(true), vf.createValue(false)},
                NAME_LONGS, new Value[] {vf.createValue("wrong_type")},
                REP_PREFIXES, new Value[] {vf.createValue(true)}
        );
        for (Map.Entry<String, Value[]> entry : invalid.entrySet()) {
            String name = entry.getKey();
            try {
                provider.createRestriction("/testPath", name, entry.getValue());
                fail("invalid restriction " + name);
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test
    public void testReadRestrictions() {
        Tree aceTree = getAceTree(NT_PREFIXES_RESTRICTION, MANDATORY_BOOLEAN_RESTRICTION, UNKNOWN_RESTRICTION);

        Set<Restriction> restrictions = provider.readRestrictions("/test", aceTree);
        assertEquals(2, restrictions.size());
        for (Restriction r : restrictions) {
            String name = r.getDefinition().getName();
            if (!supported.contains(name)) {
                fail("read unsupported restriction");
            }
        }
    }

    @Test
    public void testWriteEmptyRestrictions() throws Exception {
        Tree acTree = getAceTree();
        provider.writeRestrictions("/test", acTree, Collections.emptySet());
        verifyNoInteractions(rp1, rp2);
    }

    @Test
    public void testWriteRestrictions() throws Exception {
        Tree aceTree = getAceTree();
        provider.writeRestrictions("/test", aceTree, Set.of(LONGS_RESTRICTION, GLOB_RESTRICTION));
        verify(rp1, times(1)).writeRestrictions("/test", aceTree, Collections.singleton(GLOB_RESTRICTION));
        verify(rp2, times(1)).writeRestrictions("/test", aceTree, Collections.singleton(LONGS_RESTRICTION));
    }

    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsMissingMandatory() throws Exception {
        Tree aceTree = getAceTree(GLOB_RESTRICTION);
        provider.validateRestrictions("/test", aceTree);
    }

    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsWrongType() throws Exception {
        Tree aceTree = getAceTree(new RestrictionImpl(PropertyStates.createProperty(MANDATORY_BOOLEAN_RESTRICTION.getDefinition().getName(), "string"), true));
        provider.validateRestrictions("/test", aceTree);
    }

    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsInvalidDefinition() throws Exception {
        Restriction rWithInvalidDefinition = new RestrictionImpl(PropertyStates.createProperty(REP_GLOB, ImmutableList.of("str", "str2"), Type.STRINGS), false);
        Tree aceTree = getAceTree(rWithInvalidDefinition, MANDATORY_BOOLEAN_RESTRICTION);

        RestrictionProvider rp = createRestrictionProvider(null, rWithInvalidDefinition, GLOB_RESTRICTION.getDefinition());
        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(rp, rp2);
        cp.validateRestrictions("/test", aceTree);
    }

    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsUnsupported() throws Exception {
        Tree aceTree = getAceTree(UNKNOWN_RESTRICTION, NT_PREFIXES_RESTRICTION);

        RestrictionProvider rp = createRestrictionProvider(null, UNKNOWN_RESTRICTION, GLOB_RESTRICTION.getDefinition());
        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(rp, rp2);
        cp.validateRestrictions("/test", aceTree);
    }

    @Test
    public void testValidateRestrictions() throws Exception {
        Tree aceTree = getAceTree(LONGS_RESTRICTION, MANDATORY_BOOLEAN_RESTRICTION);
        provider.validateRestrictions("/test", aceTree);
    }

    @Test
    public void testValidateRestrictionsTreeNotExisting() throws Exception {
        Tree aceTree = getAceTree(NT_PREFIXES_RESTRICTION);
        when(aceTree.getChild(REP_RESTRICTIONS).exists()).thenReturn(false);

        CompositeRestrictionProvider.newInstance(
                rp1,
                createRestrictionProvider(LONGS_RESTRICTION.getDefinition())
        ).validateRestrictions("/test", aceTree);
    }

    @Test
    public void testValidateRestrictionsMissingProperty() throws Exception {
        Tree aceTree = getAceTree();
        when(aceTree.getChild(REP_RESTRICTIONS).exists()).thenReturn(true);

        CompositeRestrictionProvider.newInstance(
                rp1,
                createRestrictionProvider(null, GLOB_RESTRICTION, LONGS_RESTRICTION.getDefinition())
        ).validateRestrictions("/test", aceTree);
    }

    @Test
    public void testValidateRestrictionsOnAceNode() throws Exception {
        List<PropertyState> properties = new ArrayList<>();

        Tree aceTree = getAceTree();
        properties.add(aceTree.getProperty(JcrConstants.JCR_PRIMARYTYPE));

        when(aceTree.getChild(REP_RESTRICTIONS).exists()).thenReturn(false);

        when(aceTree.hasProperty(NAME_BOOLEAN)).thenReturn(true);
        when(aceTree.getProperty(NAME_BOOLEAN)).thenReturn(MANDATORY_BOOLEAN_RESTRICTION.getProperty());
        properties.add(MANDATORY_BOOLEAN_RESTRICTION.getProperty());
        when(aceTree.getProperties()).thenReturn((Iterable)properties);

        provider.validateRestrictions("/test", aceTree);
    }

    @Test
    public void testGetRestrictionPatternEmptyComposite() {
        assertSame(RestrictionPattern.EMPTY, CompositeRestrictionProvider.newInstance().getPattern("/test", Set.of(GLOB_RESTRICTION)));
    }


    @Test
    public void testGetRestrictionPatternSingleEmpty() {
        assertSame(RestrictionPattern.EMPTY, CompositeRestrictionProvider.newInstance(
                createRestrictionProvider(RestrictionPattern.EMPTY, null)).
                getPattern("/test", Set.of(GLOB_RESTRICTION)));
    }

    @Test
    public void testGetRestrictionPatternAllEmpty() {
        assertSame(RestrictionPattern.EMPTY, CompositeRestrictionProvider.newInstance(
                createRestrictionProvider(RestrictionPattern.EMPTY, null),
                createRestrictionProvider(RestrictionPattern.EMPTY, null)).
                getPattern("/test", getAceTree(NT_PREFIXES_RESTRICTION)));
    }

    @Test
    public void testGetRestrictionPattern() {
        RestrictionPattern pattern = mock(RestrictionPattern.class);
        RestrictionProvider rp1 = spy(createRestrictionProvider(pattern, null, LONGS_RESTRICTION.getDefinition()));
        RestrictionProvider rp2 = spy(createRestrictionProvider(RestrictionPattern.EMPTY, null, GLOB_RESTRICTION.getDefinition()));
        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(rp1, rp2);
        
        assertSame(pattern, cp.getPattern("/test", getAceTree(LONGS_RESTRICTION)));
        assertSame(pattern, cp.getPattern("/test", getAceTree(GLOB_RESTRICTION)));
        
        verify(rp1, never()).readRestrictions(anyString(), any(Tree.class));
        verify(rp2, never()).readRestrictions(anyString(), any(Tree.class));
    }

    @Test
    public void testGetCompositeRestrictionPattern() {
        RestrictionProvider rp1 = spy(createRestrictionProvider(mock(RestrictionPattern.class), null, NT_PREFIXES_RESTRICTION.getDefinition()));
        RestrictionProvider rp2 = spy(createRestrictionProvider(mock(RestrictionPattern.class), null, MANDATORY_BOOLEAN_RESTRICTION.getDefinition()));

        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(rp1, rp2);
        assertTrue(cp.getPattern("/test", getAceTree(LONGS_RESTRICTION)) instanceof CompositePattern);
        
        verify(rp1, never()).readRestrictions(anyString(), any(Tree.class));
        verify(rp2, never()).readRestrictions(anyString(), any(Tree.class));
    }
    
    @Test
    public void testGetRestrictionPatternFromSet() {
        Restriction r = mock(Restriction.class);
        RestrictionProvider rp1 = spy(createRestrictionProvider(mock(RestrictionPattern.class), r, NT_PREFIXES_RESTRICTION.getDefinition()));
        RestrictionProvider rp2 = spy(createRestrictionProvider(mock(RestrictionPattern.class), r, MANDATORY_BOOLEAN_RESTRICTION.getDefinition()));

        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(rp1, rp2);
        reset(rp1, rp2);
        
        RestrictionPattern pattern = cp.getPattern("/test", Collections.singleton(r));

        assertTrue(pattern instanceof CompositePattern);
        verify(rp1).getPattern(anyString(), any(Set.class));
        verify(rp2).getPattern(anyString(), any(Set.class));
        verifyNoMoreInteractions(rp1, rp2);
        reset(rp1, rp2);

    }

    @Test
    public void testGetRestrictionPatternFromSetWithEmptyPattern() {
        Restriction r = mock(Restriction.class);
        RestrictionPattern p = mock(RestrictionPattern.class);
        
        RestrictionProvider rp1 = spy(createRestrictionProvider(p, r, NT_PREFIXES_RESTRICTION.getDefinition()));
        RestrictionProvider rp3 = spy(createRestrictionProvider(RestrictionPattern.EMPTY, r, MANDATORY_BOOLEAN_RESTRICTION.getDefinition()));

        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(rp1, rp3);
        reset(rp1, rp3);
        
        RestrictionPattern pattern = cp.getPattern("/test", Collections.singleton(r));

        assertFalse(pattern instanceof CompositePattern);
        assertSame(p, pattern);
        verify(rp1).getPattern(anyString(), any(Set.class));
        verify(rp3).getPattern(anyString(), any(Set.class));
        verifyNoMoreInteractions(rp1, rp3);
    }
    
    @Test
    public void testAggregateNotAware() throws Exception {
        RestrictionProvider provider1 = mock(RestrictionProvider.class);
        RestrictionProvider provider2 = mock(RestrictionProvider.class);
        
        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(provider1, provider2);
        cp.validateRestrictions("/test", mock(Tree.class));
        
        doThrow(new AccessControlException("unsupportedrestriction")).when(provider1).validateRestrictions(anyString(), any(Tree.class));
        try {
            cp.validateRestrictions("/test", mock(Tree.class));
            fail();
        } catch (AccessControlException e) {
            // success
        }
    }
}
