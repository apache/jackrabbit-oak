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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class AbstractRestrictionProviderTest implements AccessControlConstants {

    private final String unsupportedPath = null;
    private final String testPath = "/testRoot";

    private Value globValue;
    private Value[] nameValues;
    private Value nameValue;

    private final NamePathMapper namePathMapper = NamePathMapper.DEFAULT;
    private PartialValueFactory valueFactory;
    private Map<String, ? extends RestrictionDefinition> supported;
    private AbstractRestrictionProvider restrictionProvider;

    @Before
    public void before() throws Exception {
        valueFactory = new PartialValueFactory(namePathMapper);
        globValue = valueFactory.createValue("*");
        nameValue = valueFactory.createValue("nt:file", PropertyType.NAME);
        nameValues = new Value[] {
                valueFactory.createValue("nt:folder", PropertyType.NAME),
                valueFactory.createValue("nt:file", PropertyType.NAME)
        };

        RestrictionDefinition glob = new RestrictionDefinitionImpl(REP_GLOB, Type.STRING, false);
        RestrictionDefinition nts  = new RestrictionDefinitionImpl(REP_NT_NAMES, Type.NAMES, false);
        RestrictionDefinition mand = new RestrictionDefinitionImpl("mandatory", Type.BOOLEAN, true);
        RestrictionDefinition undef = mock(RestrictionDefinition.class);
        when(undef.getName()).thenReturn("undefined");
        when(undef.getRequiredType()).thenReturn((Type) Type.UNDEFINED);

        supported = Map.of(glob.getName(), glob, nts.getName(), nts, mand.getName(), mand, undef.getName(), undef);
        restrictionProvider = new AbstractRestrictionProvider(supported) {
            @NotNull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
                throw new UnsupportedOperationException();
            }

            @NotNull
            @Override
            public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Tree getAceTree(Restriction... restrictions) {
        Tree restrictionsTree = Mockito.mock(Tree.class);;
        when(restrictionsTree.getName()).thenReturn(REP_RESTRICTIONS);
        PropertyState primaryType = PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_RESTRICTIONS, Type.NAME);
        when(restrictionsTree.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(primaryType);

        List<PropertyState> properties = new ArrayList<>();
        for (Restriction r : restrictions) {
            when(restrictionsTree.getProperty(r.getDefinition().getName())).thenReturn(r.getProperty());
            properties.add(r.getProperty());
        }
        properties.add(primaryType);
        properties.add(PropertyStates.createProperty(Iterables.get(AccessControlConstants.ACE_PROPERTY_NAMES, 0), "value"));

        when(restrictionsTree.getProperties()).thenReturn((Iterable)properties);
        when(restrictionsTree.exists()).thenReturn(true);

        Tree ace = Mockito.mock(Tree.class);
        when(ace.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_GRANT_ACE, Type.NAME));
        when(ace.getChild(REP_RESTRICTIONS)).thenReturn(restrictionsTree);
        when(ace.exists()).thenReturn(true);

        return ace;
    }

    @Test
    public void testGetSupportedRestrictions() {
        Set<RestrictionDefinition> defs = restrictionProvider.getSupportedRestrictions(testPath);
        assertNotNull(defs);
        assertEquals(supported.size(), defs.size());
        for (RestrictionDefinition def : supported.values()) {
            assertTrue(defs.contains(def));
        }
    }

    @Test
    public void testGetSupportedRestrictionsForUnsupportedPath() {
        Set<RestrictionDefinition> defs = restrictionProvider.getSupportedRestrictions(unsupportedPath);
        assertNotNull(defs);
        assertTrue(defs.isEmpty());
    }

    @Test(expected = AccessControlException.class)
    public void testCreateForUnsupportedPath() throws Exception {
        restrictionProvider.createRestriction(unsupportedPath, REP_GLOB, globValue);
    }

    @Test(expected = AccessControlException.class)
    public void testCreateForUnsupportedName() throws Exception {
        restrictionProvider.createRestriction(testPath, "unsupported", nameValue);
    }

    @Test(expected = AccessControlException.class)
    public void testCreateForUnsupportedType() throws Exception {
        restrictionProvider.createRestriction(testPath, REP_NT_NAMES,
                valueFactory.createValue("nt:file", PropertyType.NAME),
                valueFactory.createValue(true));
    }

    @Test(expected = AccessControlException.class)
    public void testCreateForUnsupportedMultiValues() throws Exception {
        restrictionProvider.createRestriction(testPath, REP_GLOB,
                valueFactory.createValue("*"),
                valueFactory.createValue("/a/*"));
    }

    @Test
    public void testCreateRestriction() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        assertNotNull(r);
        assertEquals(REP_GLOB, r.getDefinition().getName());
        assertEquals(globValue.getString(), r.getProperty().getValue(Type.STRING));
    }

    @Test
    public void testCreateRestrictionFromArray() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_GLOB, new Value[] {globValue});
        assertNotNull(r);
        assertEquals(REP_GLOB, r.getDefinition().getName());
        assertEquals(globValue.getString(), r.getProperty().getValue(Type.STRING));
        assertFalse(r.getProperty().isArray());
    }

    @Test
    public void testCreateMvRestriction() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_NT_NAMES,
                valueFactory.createValue("nt:folder", PropertyType.NAME),
                valueFactory.createValue("nt:file", PropertyType.NAME));
        assertNotNull(r);
        assertEquals(REP_NT_NAMES, r.getDefinition().getName());
        assertEquals(Type.NAMES, r.getDefinition().getRequiredType());

        PropertyState ps = r.getProperty();
        assertTrue(ps.isArray());
        assertEquals(Type.NAMES, ps.getType());

        List<Value> vs = valueFactory.createValues(ps);
        assertArrayEquals(nameValues, vs.toArray(new Value[0]));
    }

    @Test
    public void testCreateMvRestriction2() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_NT_NAMES, nameValues);
        assertNotNull(r);
        assertEquals(REP_NT_NAMES, r.getDefinition().getName());
        assertEquals(Type.NAMES, r.getDefinition().getRequiredType());

        PropertyState ps = r.getProperty();
        assertTrue(ps.isArray());
        assertEquals(Type.NAMES, ps.getType());

        List<Value> vs = valueFactory.createValues(ps);
        assertArrayEquals(nameValues, vs.toArray(new Value[0]));
    }

    @Test
    public void testCreateMvRestriction3() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_NT_NAMES, nameValue);
        assertNotNull(r);
        assertEquals(REP_NT_NAMES, r.getDefinition().getName());
        assertEquals(Type.NAMES, r.getDefinition().getRequiredType());

        assertTrue(r.getProperty().isArray());
        assertEquals(Type.NAMES, r.getProperty().getType());

        List<Value> vs = valueFactory.createValues(r.getProperty());
        assertArrayEquals(new Value[] {nameValue}, vs.toArray(new Value[0]));
    }

    @Test
    public void testCreateEmptyMvRestriction() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_NT_NAMES);
        assertNotNull(r);
        assertEquals(REP_NT_NAMES, r.getDefinition().getName());
        assertEquals(Type.NAMES, r.getDefinition().getRequiredType());

        assertTrue(r.getProperty().isArray());
        assertEquals(Type.NAMES, r.getProperty().getType());

        List<Value> vs = valueFactory.createValues(r.getProperty());
        assertNotNull(vs);
        assertEquals(0, vs.size());
    }

    @Test
    public void testCreateEmptyMvRestriction2() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_NT_NAMES);
        assertNotNull(r);
        assertEquals(REP_NT_NAMES, r.getDefinition().getName());
        assertEquals(Type.NAMES, r.getDefinition().getRequiredType());

        assertTrue(r.getProperty().isArray());
        assertEquals(Type.NAMES, r.getProperty().getType());

        List<Value> vs = valueFactory.createValues(r.getProperty());
        assertNotNull(vs);
        assertEquals(0, vs.size());
    }

    @Test
    public void testCreatedUndefinedType() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, "undefined", valueFactory.createValue(23));
        assertNotNull(r);
        assertEquals(Type.UNDEFINED, r.getDefinition().getRequiredType());
        assertEquals(Type.LONG, r.getProperty().getType());
    }

    @Test(expected = AccessControlException.class)
    public void testCreateUndefinedTypeMV() throws Exception {
        Restriction r2 = restrictionProvider.createRestriction(testPath, "undefined", valueFactory.createValue(23), valueFactory.createValue(false));
    }

    @Test
    public void testReadRestrictionsForUnsupportedPath() {
        Set<Restriction> restrictions = restrictionProvider.readRestrictions(unsupportedPath, getAceTree());
        assertTrue(restrictions.isEmpty());
    }

    @Test
    public void testReadRestrictions() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        Tree aceTree = getAceTree(r);

        Set<Restriction> restrictions = restrictionProvider.readRestrictions(testPath, aceTree);
        assertEquals(1, restrictions.size());
        assertTrue(restrictions.contains(r));
    }

    @Test
    public void testReadRestrictionsWithTypeMismatch() {
        PropertyState wrongType = PropertyStates.createProperty(REP_GLOB, 24);
        Restriction r = new Restriction() {

            @Override
            public @NotNull RestrictionDefinition getDefinition() {
                return mock(RestrictionDefinition.class);
            }

            @Override
            public @NotNull PropertyState getProperty() {
                return wrongType;
            }
        };
        Tree aceTree = getAceTree(r);

        Set<Restriction> restrictions = restrictionProvider.readRestrictions(testPath, aceTree);
        assertTrue(restrictions.isEmpty());
    }

    @Test
    public void testValidateRestrictionsUnsupportedPathEmptyRestrictions() throws Exception {
        // empty restrictions => must succeed
        restrictionProvider.validateRestrictions(null, getAceTree());
    }

    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsUnsupportedPath() throws Exception {
        // non-empty restrictions => must fail
        Restriction restr = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        restrictionProvider.validateRestrictions(null, getAceTree(restr));
    }

    @Test
    public void testValidateRestrictionsUnsupportedPathInComposite() throws Exception {
        // different provider that supports rep:glob at the 'null' path
        RestrictionProvider second = createSecondProvider(Collections.singletonMap(REP_GLOB, supported.get(REP_GLOB)), s -> false);

        Restriction restr = second.createRestriction(null, REP_GLOB, globValue);
        
        // non-empty restrictions at unsupported path => must not fail if covered by a different provider
        RestrictionProvider composite = CompositeRestrictionProvider.newInstance(restrictionProvider, second);
        composite.validateRestrictions(null, getAceTree(restr));
    }
    
    @Test
    public void testValidateRestrictionsUnsupportedPathInComposite2() throws Exception {
        Restriction restr = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        Restriction mandatory = restrictionProvider.createRestriction(testPath, "mandatory", valueFactory.createValue(false));

        // create a second provider that does not support restrictions at 'testpath'
        RestrictionProvider second = createSecondProvider(Collections.singletonMap(REP_GLOB, supported.get(REP_GLOB)), testPath::equals);

        RestrictionProvider composite = CompositeRestrictionProvider.newInstance(restrictionProvider, second);
        composite.validateRestrictions(testPath, getAceTree(restr, mandatory));
    }
    
    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsUnsupportedPathInComposite3() throws Exception {
        Restriction restr = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        Restriction mandatory = restrictionProvider.createRestriction(testPath, "mandatory", valueFactory.createValue(false));

        // create a second provider that does support rep:glob at null path (but not the 'mandatory' restriction)
        RestrictionProvider second = createSecondProvider(Collections.singletonMap(REP_GLOB, supported.get(REP_GLOB)), s -> false);

        // since 'mandatory' restriction is not supported the validation must fail
        RestrictionProvider composite = CompositeRestrictionProvider.newInstance(restrictionProvider, second);
        composite.validateRestrictions(null, getAceTree(restr, mandatory));
    }
    
    @NotNull
    private static RestrictionProvider createSecondProvider(@NotNull Map<String, RestrictionDefinition> supported, 
                                                            @NotNull Function<String, Boolean> isUnsupportedFunction) {
        return new AbstractRestrictionProvider(supported) {
            @Override
            protected boolean isUnsupportedPath(@Nullable String oakPath) {
                return isUnsupportedFunction.apply(oakPath);
            }

            @Override
            public @NotNull RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
                throw new UnsupportedOperationException();
            }

            @Override
            public @NotNull RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsWrongType() throws Exception {
        Restriction mand = restrictionProvider.createRestriction(testPath, "mandatory", valueFactory.createValue(true));
        Tree ace = getAceTree(mand, new RestrictionImpl(PropertyStates.createProperty(REP_GLOB, true), false));
        restrictionProvider.validateRestrictions(testPath, ace);
    }

    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsUnsupportedRestriction() throws Exception {
        Restriction mand = restrictionProvider.createRestriction(testPath, "mandatory", valueFactory.createValue(true));
        Tree ace = getAceTree(mand, new RestrictionImpl(PropertyStates.createProperty("unsupported", "value"), false));
        restrictionProvider.validateRestrictions(testPath, ace);
    }

    @Test(expected = AccessControlException.class)
    public void testValidateRestrictionsMissingMandatory() throws Exception {
        Restriction glob = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        restrictionProvider.validateRestrictions(testPath, getAceTree(glob));
    }

    @Test
    public void testValidateRestrictions() throws Exception {
        Restriction glob = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        Restriction ntNames = restrictionProvider.createRestriction(testPath, REP_NT_NAMES, nameValues);
        Restriction mand = restrictionProvider.createRestriction(testPath, "mandatory", valueFactory.createValue(true));

        restrictionProvider.validateRestrictions(testPath, getAceTree(mand));
        restrictionProvider.validateRestrictions(testPath, getAceTree(mand, glob));
        restrictionProvider.validateRestrictions(testPath, getAceTree(mand, ntNames));
        restrictionProvider.validateRestrictions(testPath, getAceTree(mand, glob, ntNames));
    }

    @Test
    public void testGetRestrictionTree() {
        Tree aceTree = getAceTree();
        Tree restrictionTree = restrictionProvider.getRestrictionsTree(aceTree);
        assertEquals(aceTree.getChild(REP_RESTRICTIONS), restrictionTree);
    }

    @Test
    public void testGetRestrictionTreeMissing() {
        Tree aceTree = when(mock(Tree.class).getChild(REP_RESTRICTIONS)).thenReturn(mock(Tree.class)).getMock();
        Tree restrictionTree = restrictionProvider.getRestrictionsTree(aceTree);
        assertEquals(aceTree, restrictionTree);
    }

    @Test
    public void testWriteEmptyRestrictions() throws Exception {
        Tree aceTree = getAceTree();
        restrictionProvider.writeRestrictions(null, aceTree, Collections.emptySet());
        verifyNoInteractions(aceTree);
    }

    @Test
    public void testWriteRestrictions() throws Exception {
        Restriction ntNames = restrictionProvider.createRestriction(testPath, REP_NT_NAMES, nameValues);
        Tree aceTree = getAceTree();
        restrictionProvider.writeRestrictions(null, aceTree, Collections.singleton(ntNames));

        verify(aceTree, times(1)).getChild(REP_RESTRICTIONS);
        verify(aceTree.getChild(REP_RESTRICTIONS), times(1)).setProperty(ntNames.getProperty());
    }
}
