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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.RESIDUAL_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CurrentPatternTest {
    
    private static final String TEST_PATH = "/test/path";
    private static final String PROP_NAME = "prop";
    
    @NotNull
    private static RestrictionPattern createPattern(@NotNull String... propertyNames) {
        return new CurrentPattern(TEST_PATH, Arrays.asList(propertyNames));
    }
    
    @NotNull
    private static Tree mockTree(@NotNull String path) {
        return when(mock(Tree.class).getPath()).thenReturn(path).getMock();
    }

    @NotNull
    private static PropertyState mockProperty(@NotNull String name) {
        return when(mock(PropertyState.class).getName()).thenReturn(name).getMock();
    }
    
    @Test
    public void testMatches() {
        assertFalse(createPattern().matches());
        assertFalse(createPattern(RESIDUAL_NAME).matches());
        assertFalse(createPattern(PROP_NAME).matches());
        assertFalse(createPattern("a", "b", "c").matches());
    }

    @Test
    public void testMatchesTreePropertyPathMismatch() {
        Tree t = mockTree(TEST_PATH + "/mismatch");
        PropertyState p = mockProperty(PROP_NAME);

        RestrictionPattern rp = createPattern();
        assertFalse(rp.matches(t, null));
        assertFalse(rp.matches(t, p));

        rp = createPattern(PROP_NAME);
        assertFalse(rp.matches(t, null));
        assertFalse(rp.matches(t, p));
        
        rp = createPattern(RESIDUAL_NAME);
        assertFalse(rp.matches(t, null));
        assertFalse(rp.matches(t, p));
        
        verify(t, times(6)).getPath();
        verify(p, times(3)).getName();
        verifyNoMoreInteractions(t,p);
    }

    @Test
    public void testMatchesTreePropertyNoPropertyNames() {
        Tree t = mockTree(TEST_PATH);
        PropertyState p = mockProperty(PROP_NAME);
        
        RestrictionPattern rp = createPattern();
        assertTrue(rp.matches(t, null));
        assertFalse(rp.matches(t, p));
        
        verify(t, times(2)).getPath();
        verify(p).getName();
        verifyNoMoreInteractions(t, p);
    }

    @Test
    public void testMatchesTreePropertyAllPropertyNames() {
        Tree t = mockTree(TEST_PATH);
        PropertyState p = mockProperty(PROP_NAME);

        RestrictionPattern rp = createPattern(RESIDUAL_NAME);
        assertTrue(rp.matches(t, null));
        assertTrue(rp.matches(t, p));

        verify(t, times(2)).getPath();
        verify(p).getName();
        verifyNoMoreInteractions(t, p);
    }

    @Test
    public void testMatchesTreePropertyPropertyNames() {
        Tree t = mockTree(TEST_PATH);
        PropertyState p = mockProperty(PROP_NAME);
        PropertyState p2 = mockProperty("a");

        // matching property names
        RestrictionPattern rp = createPattern(PROP_NAME, "a");
        assertTrue(rp.matches(t, null));
        assertTrue(rp.matches(t, p));
        assertTrue(rp.matches(t, p2));

        // property names don't match
        rp = createPattern("other");
        assertTrue(rp.matches(t, null));
        assertFalse(rp.matches(t, p));
        assertFalse(rp.matches(t, p2));

        verify(t, times(6)).getPath();
        verify(p, times(2)).getName();
        verify(p2, times(2)).getName();
        verifyNoMoreInteractions(t, p, p2);
    }

    @Test
    public void testMatchesPath() {
        List<String[]> propNames = ImmutableList.of(new String[0], new String[]{RESIDUAL_NAME}, new String[] {PROP_NAME});
        for (String[] pn : propNames) {
            RestrictionPattern rp = createPattern(pn);
            assertTrue(rp.matches(TEST_PATH));
            assertFalse(rp.matches("/another/path"));
            assertFalse(rp.matches(PathUtils.ROOT_PATH));
            assertFalse(rp.matches(TEST_PATH + "/" + PROP_NAME));
        }
    }

    @Test
    public void testMatchesPathPointsToKnownProperty() {
        String[] knownPropertyNames = new String[] {
                JcrConstants.JCR_PRIMARYTYPE, 
                AccessControlConstants.REP_PRINCIPAL_NAME,
                AtomicCounterEditor.PROP_COUNTER};

        // pattern without any property-names will not match any of the paths.
        RestrictionPattern rp = createPattern();
        for (String pn : knownPropertyNames) {
            assertFalse(rp.matches(PathUtils.concat(TEST_PATH, pn)));
        }
        
        // pattern with * or all propnames will match
        List<String[]> propNames = ImmutableList.of(new String[]{RESIDUAL_NAME}, knownPropertyNames);
        for (String[] names : propNames) {
            rp = createPattern(names);
            for (String pn : knownPropertyNames) {
                assertTrue(rp.matches(PathUtils.concat(TEST_PATH, pn)));
            }
            assertFalse(rp.matches(PathUtils.concat(TEST_PATH, "otherPrefix:propName")));
            assertFalse(rp.matches(PathUtils.concat(TEST_PATH, "propName")));
        }

        // pattern with different set of propnames won't match
        rp = createPattern(JcrConstants.JCR_DATA);
        for (String pn : knownPropertyNames) {
            assertFalse(rp.matches(PathUtils.concat(TEST_PATH, pn)));
            assertFalse(rp.matches(PathUtils.concat(TEST_PATH, "otherPrefix:propName")));
            assertFalse(rp.matches(PathUtils.concat(TEST_PATH, "propName")));
        }
    }

    @Test
    public void testMatchesPathPointsToKnownNode() {
        String[] knownNodeNames = new String[] {
                JcrConstants.JCR_CONTENT,
                AccessControlConstants.REP_POLICY,
                IndexConstants.INDEX_DEFINITIONS_NAME};

        // pattern without any property-names will not match any of the paths.
        RestrictionPattern rp = createPattern();
        for (String pn : knownNodeNames) {
            assertFalse(rp.matches(PathUtils.concat(TEST_PATH, pn)));
        }

        // pattern with * or all node-names will NOT match because they point to items that are known to be nodes
        List<String[]> propNames = ImmutableList.of(new String[]{RESIDUAL_NAME}, knownNodeNames);
        for (String[] names : propNames) {
            rp = createPattern(names);
            for (String pn : knownNodeNames) {
                assertFalse(rp.matches(PathUtils.concat(TEST_PATH, pn)));
            }
        }
    }

    @Test
    public void testMatchesPathIsPropertyFalse() {
        RestrictionPattern rp = createPattern();
        assertTrue(rp.matches(TEST_PATH, false));
        assertFalse(rp.matches("/another/path", false));
        assertFalse(rp.matches(PathUtils.ROOT_PATH, false));
        assertFalse(rp.matches(TEST_PATH + "/" + PROP_NAME, false));

        rp = createPattern(RESIDUAL_NAME);
        assertTrue(rp.matches(TEST_PATH, false));
        assertFalse(rp.matches("/another/path", false));
        assertFalse(rp.matches(PathUtils.ROOT_PATH, false));
        assertFalse(rp.matches(TEST_PATH + "/" + PROP_NAME, false));

        rp = createPattern(PROP_NAME);
        assertTrue(rp.matches(TEST_PATH, false));
        assertFalse(rp.matches("/another/path", false));
        assertFalse(rp.matches(PathUtils.ROOT_PATH, false));
        assertFalse(rp.matches(TEST_PATH + "/" + PROP_NAME, false));
    }

    @Test
    public void testMatchesPathIsPropertyTrue() {
        RestrictionPattern rp = createPattern();
        assertFalse(rp.matches(TEST_PATH, true));
        assertFalse(rp.matches("/another/path", true));
        assertFalse(rp.matches(PathUtils.ROOT_PATH, true));
        assertFalse(rp.matches(TEST_PATH + "/" + PROP_NAME, true));

        rp = createPattern(RESIDUAL_NAME);
        assertFalse(rp.matches(TEST_PATH, true));
        assertFalse(rp.matches("/another/path", true));
        assertFalse(rp.matches(PathUtils.ROOT_PATH, true));
        assertTrue(rp.matches(TEST_PATH + "/" + PROP_NAME, true));
        assertTrue(rp.matches(TEST_PATH + "/another", true));

        rp = createPattern(PROP_NAME);
        assertFalse(rp.matches(TEST_PATH, true));
        assertFalse(rp.matches("/another/path", true));
        assertFalse(rp.matches(PathUtils.ROOT_PATH, true));
        assertTrue(rp.matches(TEST_PATH + "/" + PROP_NAME, true));
        assertFalse(rp.matches(TEST_PATH + "/another", true));
    }

    @Test
    public void testToString() {
        assertEquals(createPattern(PROP_NAME).toString(), createPattern(PROP_NAME).toString());
        assertNotEquals(createPattern(RESIDUAL_NAME).toString(), createPattern(PROP_NAME).toString());
        assertNotEquals(createPattern(RESIDUAL_NAME).toString(), createPattern().toString());
    }

    @Test
    public void testHashCode() {
        RestrictionPattern rp = createPattern(PROP_NAME);
        assertEquals(rp.hashCode(), createPattern(PROP_NAME).hashCode());
        assertNotEquals(rp.hashCode(), createPattern().hashCode());
        assertNotEquals(rp.hashCode(), createPattern(RESIDUAL_NAME).hashCode());
        assertNotEquals(rp.hashCode(), createPattern(PROP_NAME, "a", "b").hashCode());
    }

    @Test
    public void testEquals() {
        RestrictionPattern rp = createPattern(PROP_NAME, "a", "b");
        assertEquals(rp, rp);
        assertEquals(rp, createPattern(PROP_NAME, "a", "b"));
    }

    @Test
    public void testNotEquals() {
        RestrictionPattern rp = createPattern(RESIDUAL_NAME);
        // different tree path
        assertNotEquals(rp, new CurrentPattern("/another/path", Collections.singleton(RESIDUAL_NAME)));
        // different set of prop names
        assertNotEquals(rp, createPattern());
        assertNotEquals(rp, createPattern(PROP_NAME));
        // different restrictions
        assertNotEquals(rp, new ItemNamePattern(ImmutableSet.of("a", "b")));
        assertNotEquals(rp, new PrefixPattern(ImmutableSet.of("a", "b", "c")));
    }
}
