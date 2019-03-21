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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeRestrictionProviderTest implements AccessControlConstants {

    private static final String NAME_LONGS = "longs";
    private static final String NAME_BOOLEAN = "boolean";

    private static final Restriction GLOB_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(REP_GLOB, "*"), false);
    private static final Restriction NT_PREFIXES_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(REP_PREFIXES, ImmutableList.of(), Type.STRINGS), false);
    private static final Restriction MANDATORY_BOOLEAN_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(NAME_BOOLEAN, true, Type.BOOLEAN), true);
    private static final Restriction LONGS_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(NAME_LONGS, ImmutableList.of(Long.MAX_VALUE), Type.LONGS), false);
    private static final Restriction UNKNOWN_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty("unknown", "string"), false);

    private RestrictionProvider rp1 = spy(createRestrictionProvider(GLOB_RESTRICTION.getDefinition(), NT_PREFIXES_RESTRICTION.getDefinition()));
    private RestrictionProvider rp2 = spy(createRestrictionProvider(MANDATORY_BOOLEAN_RESTRICTION.getDefinition(), LONGS_RESTRICTION.getDefinition()));

    private Set<String> supported = ImmutableSet.of(
            MANDATORY_BOOLEAN_RESTRICTION.getDefinition().getName(),
            LONGS_RESTRICTION.getDefinition().getName(),
            REP_PREFIXES,
            REP_GLOB);
    private RestrictionProvider provider = CompositeRestrictionProvider.newInstance(rp1, rp2);

    private ValueFactory vf = new ValueFactoryImpl(mock(Root.class), NamePathMapper.DEFAULT);

    @NotNull
    private AbstractRestrictionProvider createRestrictionProvider(@NotNull RestrictionDefinition... supportedDefinitions) {
        return createRestrictionProvider(null, null, supportedDefinitions);
    }

    @NotNull
    private AbstractRestrictionProvider createRestrictionProvider(@Nullable RestrictionPattern pattern, @Nullable Restriction toRead, @NotNull RestrictionDefinition... supportedDefinitions) {
        ImmutableMap.Builder<String, RestrictionDefinition> builder = ImmutableMap.builder();
        for (RestrictionDefinition def : supportedDefinitions) {
            builder.put(def.getName(), def);
        }
        return new AbstractRestrictionProvider(builder.build()) {
            @Override
            public @NotNull Set<Restriction> readRestrictions(String oakPath, @NotNull Tree aceTree) {
                if (toRead != null) {
                    return ImmutableSet.of(toRead);
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
        Tree restrictionsTree = mock(Tree.class);;
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
        assertSame(RestrictionProvider.EMPTY, CompositeRestrictionProvider.newInstance(Collections.<RestrictionProvider>emptySet()));
    }

    @Test
    public void testSingle() {
        assertSame(rp1, CompositeRestrictionProvider.newInstance(Collections.singleton(rp1)));
    }

    @Test
    public void testNewInstance() {
        RestrictionProvider crp = CompositeRestrictionProvider.newInstance(ImmutableSet.of(rp1, rp2));
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
        Map<String, Value> valid = ImmutableMap.of(
                NAME_BOOLEAN, vf.createValue(true),
                NAME_LONGS, vf.createValue(10),
                REP_GLOB, vf.createValue("*")
        );
        for (String name : valid.keySet()) {
            provider.createRestriction("/testPath", name, valid.get(name));
        }
    }

    @Test(expected = AccessControlException.class)
    public void testCreateRestrictionWithInvalidPath() throws Exception {
        provider.createRestriction(null, REP_GLOB, vf.createValue("*"));
    }

    @Test
    public void testCreateInvalidRestriction() throws Exception {
        Map<String, Value> invalid = ImmutableMap.of(
                NAME_BOOLEAN, vf.createValue("wrong_type"),
                REP_GLOB, vf.createValue(true)
        );
        for (String name : invalid.keySet()) {
            try {
                provider.createRestriction("/testPath", name, invalid.get(name));
                fail("invalid restriction " + name);
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test
    public void testMvCreateRestriction() throws RepositoryException {
        Map<String, Value[]> valid = ImmutableMap.of(
                NAME_LONGS, new Value[] {vf.createValue(100)},
                REP_PREFIXES, new Value[] {vf.createValue("prefix"), vf.createValue("prefix2")}
        );
        for (String name : valid.keySet()) {
            provider.createRestriction("/testPath", name, valid.get(name));
        }
    }

    @Test(expected = AccessControlException.class)
    public void testCreateMvRestrictionWithInvalidPath() throws Exception {
        provider.createRestriction(null, REP_PREFIXES, new Value[] {vf.createValue("jcr")});
    }

    @Test
    public void testCreateInvalidMvRestriction() throws Exception {
        Map<String, Value[]> invalid = ImmutableMap.of(
                NAME_BOOLEAN, new Value[] {vf.createValue(true), vf.createValue(false)},
                NAME_LONGS, new Value[] {vf.createValue("wrong_type")},
                REP_PREFIXES, new Value[] {vf.createValue(true)}
        );
        for (String name : invalid.keySet()) {
            try {
                provider.createRestriction("/testPath", name, invalid.get(name));
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
        provider.writeRestrictions("/test", getAceTree(), Collections.emptySet());
    }

    @Test
    public void testWriteRestrictions() throws Exception {
        Tree aceTree = getAceTree();
        provider.writeRestrictions("/test", aceTree, ImmutableSet.of(LONGS_RESTRICTION, GLOB_RESTRICTION));
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
        assertSame(RestrictionPattern.EMPTY, CompositeRestrictionProvider.newInstance().getPattern("/test", ImmutableSet.of(GLOB_RESTRICTION)));
    }


    @Test
    public void testGetRestrictionPatternSingleEmpty() {
        assertSame(RestrictionPattern.EMPTY, CompositeRestrictionProvider.newInstance(
                createRestrictionProvider(RestrictionPattern.EMPTY, null)).
                getPattern("/test", ImmutableSet.of(GLOB_RESTRICTION)));
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
        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(
                createRestrictionProvider(pattern, null, LONGS_RESTRICTION.getDefinition()),
                createRestrictionProvider(RestrictionPattern.EMPTY, null, GLOB_RESTRICTION.getDefinition()));
        assertSame(pattern, cp.getPattern("/test", getAceTree(LONGS_RESTRICTION)));
        assertSame(pattern, cp.getPattern("/test", getAceTree(GLOB_RESTRICTION)));
    }

    @Test
    public void testGetCompositeRestrictionPattern() {
        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(
                createRestrictionProvider(mock(RestrictionPattern.class), null, NT_PREFIXES_RESTRICTION.getDefinition()),
                createRestrictionProvider(mock(RestrictionPattern.class), null, MANDATORY_BOOLEAN_RESTRICTION.getDefinition()));
        assertTrue(cp.getPattern("/test", getAceTree(LONGS_RESTRICTION)) instanceof CompositePattern);
    }
}
