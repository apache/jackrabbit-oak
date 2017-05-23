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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class CompositeRestrictionProviderTest implements AccessControlConstants {

    private static final String NAME_LONGS = "longs";
    private static final String NAME_BOOLEAN = "boolean";

    private static final Restriction GLOB_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(REP_GLOB, "*"), false);
    private static final Restriction NT_PREFIXES_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(REP_PREFIXES, ImmutableList.of(), Type.STRINGS), false);
    private static final Restriction MANDATORY_BOOLEAN_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(NAME_BOOLEAN, true, Type.BOOLEAN), true);
    private static final Restriction LONGS_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty(NAME_LONGS, ImmutableList.of(Long.MAX_VALUE), Type.LONGS), false);
    private static final Restriction UNKNOWN_RESTRICTION = new RestrictionImpl(PropertyStates.createProperty("unknown", "string"), false);

    private RestrictionProvider rp1 = new AbstractRestrictionProvider(ImmutableMap.<String, RestrictionDefinition>of(
            REP_GLOB, GLOB_RESTRICTION.getDefinition(),
            REP_PREFIXES, NT_PREFIXES_RESTRICTION.getDefinition()
    )) {
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
    private RestrictionProvider rp2 = new AbstractRestrictionProvider(ImmutableMap.of(
            NAME_BOOLEAN, MANDATORY_BOOLEAN_RESTRICTION.getDefinition(),
            NAME_LONGS, LONGS_RESTRICTION.getDefinition()
    )) {
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

    private Set<String> supported = ImmutableSet.of(
            MANDATORY_BOOLEAN_RESTRICTION.getDefinition().getName(),
            LONGS_RESTRICTION.getDefinition().getName(),
            REP_PREFIXES,
            REP_GLOB);
    private RestrictionProvider provider = CompositeRestrictionProvider.newInstance(rp1, rp2);

    private ValueFactory vf = new ValueFactoryImpl(Mockito.mock(Root.class), NamePathMapper.DEFAULT);

    private Tree getAceTree(Restriction... restrictions) throws Exception {
        Tree restrictionsTree = Mockito.mock(Tree.class);;
        when(restrictionsTree.getName()).thenReturn(REP_RESTRICTIONS);
        when(restrictionsTree.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_RESTRICTIONS, Type.NAME));
        List<PropertyState> properties = new ArrayList();
        for (Restriction r : restrictions) {
            when(restrictionsTree.getProperty(r.getDefinition().getName())).thenReturn(r.getProperty());
            properties.add(r.getProperty());
        }
        when(restrictionsTree.getProperties()).thenReturn((Iterable)properties);
        when(restrictionsTree.exists()).thenReturn(true);

        Tree ace = Mockito.mock(Tree.class);
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

    @Test
    public void testCreateRestrictionWithInvalidPath() throws Exception {
        try {
            provider.createRestriction(null, REP_GLOB, vf.createValue("*"));
            fail("rep:glob not supported at 'null' path");
        } catch (AccessControlException e) {
            // success
        }
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
    public void testReadRestrictions() throws Exception {
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

        RestrictionProvider rp = new AbstractRestrictionProvider(ImmutableMap.of(REP_GLOB, GLOB_RESTRICTION.getDefinition())) {
            @Nonnull
            @Override
            public Set<Restriction> readRestrictions(String oakPath, @Nonnull Tree aceTree) {
                return ImmutableSet.of(rWithInvalidDefinition);
            }

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
        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(rp, rp2);
        cp.validateRestrictions("/test", aceTree);
    }

    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsUnsupported() throws Exception {
        Tree aceTree = getAceTree(UNKNOWN_RESTRICTION, MANDATORY_BOOLEAN_RESTRICTION);

        RestrictionProvider rp = new AbstractRestrictionProvider(ImmutableMap.of(REP_GLOB, GLOB_RESTRICTION.getDefinition())) {
            @Nonnull
            @Override
            public Set<Restriction> readRestrictions(String oakPath, @Nonnull Tree aceTree) {
                return ImmutableSet.of(UNKNOWN_RESTRICTION);
            }

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
        RestrictionProvider cp = CompositeRestrictionProvider.newInstance(rp, rp2);
        cp.validateRestrictions("/test", aceTree);
    }

    @Test
    public void testGetRestrictionPatternEmptyComposite() throws Exception {
        assertSame(RestrictionPattern.EMPTY, CompositeRestrictionProvider.newInstance().getPattern("/test", ImmutableSet.of(GLOB_RESTRICTION)));
    }


    @Test
    public void testGetRestrictionPatternSingleEmpty() throws Exception {
        assertSame(RestrictionPattern.EMPTY, CompositeRestrictionProvider.newInstance(new AbstractRestrictionProvider(ImmutableMap.of()) {
            @Nonnull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
                return RestrictionPattern.EMPTY;
            }

            @Nonnull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
                return RestrictionPattern.EMPTY;
            }
        }).getPattern("/test", ImmutableSet.of(GLOB_RESTRICTION)));
    }

    @Test
    public void testGetRestrictionPatternAllEmpty() throws Exception {
        assertSame(RestrictionPattern.EMPTY, CompositeRestrictionProvider.newInstance(new AbstractRestrictionProvider(ImmutableMap.of()) {
            @Nonnull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
                return RestrictionPattern.EMPTY;
            }

            @Nonnull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
                return RestrictionPattern.EMPTY;
            }
        }, new AbstractRestrictionProvider(ImmutableMap.of()) {
            @Nonnull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
                return RestrictionPattern.EMPTY;
            }

            @Nonnull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
                return RestrictionPattern.EMPTY;
            }
        }).getPattern("/test", getAceTree(NT_PREFIXES_RESTRICTION)));
    }
}