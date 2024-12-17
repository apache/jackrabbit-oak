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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositePattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link RestrictionProviderImpl}
 */
@RunWith(Parameterized.class)
public class RestrictionProviderImplTest extends AbstractSecurityTest implements AccessControlConstants {

    private static final String TEST_RESTR_NAME = "test";
    
    private final boolean asComposite;
    private RestrictionProvider provider;

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{false, "RestrictionProviderImpl as singular provider"},
                new Object[]{true, "RestrictionProviderImpl as part of a composite restriction provider"});
    }
    
    public RestrictionProviderImplTest(boolean asComposite, String name) {
        this.asComposite = asComposite;
    }

    @Before
    public void before() throws Exception {
        super.before();

        RestrictionProviderImpl rp = new RestrictionProviderImpl();
        if (asComposite) {
            provider = CompositeRestrictionProvider.newInstance(rp, new TestProvider(Collections.singletonMap(TEST_RESTR_NAME, new RestrictionDefinitionImpl("test", Type.STRING, false))));
        } else {
            provider = rp;
        }
    }

    @Test
    public void testGetSupportedDefinitions() {
        assertTrue(provider.getSupportedRestrictions(null).isEmpty());

        Set<RestrictionDefinition> defs = provider.getSupportedRestrictions("/testPath");
        assertNotNull(defs);
        int expectedSize = (asComposite) ? 8 : 7;
        assertEquals(expectedSize, defs.size());

        Set<String> stringsPropNames = Set.of(REP_PREFIXES, REP_CURRENT, REP_GLOBS, REP_SUBTREES);
        for (RestrictionDefinition def : defs) {
            if (REP_GLOB.equals(def.getName())) {
                assertEquals(Type.STRING, def.getRequiredType());
                assertFalse(def.isMandatory());
            } else if (REP_NT_NAMES.equals(def.getName())) {
                assertEquals(Type.NAMES, def.getRequiredType());
                assertFalse(def.isMandatory());
            } else if (REP_ITEM_NAMES.equals(def.getName())) {
                assertEquals(Type.NAMES, def.getRequiredType());
                assertFalse(def.isMandatory());
            } else if (stringsPropNames.contains(def.getName())) {
                assertEquals(Type.STRINGS, def.getRequiredType());
                assertFalse(def.isMandatory());
            } else {
                if (asComposite) {
                    assertEquals(TEST_RESTR_NAME, def.getName());
                } else {
                    fail("unexpected restriction " + def.getName());
                }
            }
        }
    }

    @Test
    public void testGetRestrictionPattern() throws Exception {
        Map<PropertyState, RestrictionPattern> map = new HashMap<>();
        map.put(PropertyStates.createProperty(REP_GLOB, "/*/jcr:content"), GlobPattern.create("/testPath", "/*/jcr:content"));
        List<String> ntNames = ImmutableList.of(JcrConstants.NT_FOLDER, JcrConstants.NT_LINKEDFILE);
        map.put(PropertyStates.createProperty(REP_NT_NAMES, ntNames, Type.NAMES), new NodeTypePattern(ntNames));

        Tree tree = TreeUtil.getOrAddChild(root.getTree("/"), "testPath", JcrConstants.NT_UNSTRUCTURED);
        Tree restrictions = TreeUtil.addChild(tree, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);

        // test restrictions individually
        for (Map.Entry<PropertyState, RestrictionPattern> entry : map.entrySet()) {
            restrictions.setProperty(entry.getKey());

            RestrictionPattern pattern = provider.getPattern("/testPath", restrictions);
            assertEquals(entry.getValue(), pattern);

            restrictions.removeProperty(entry.getKey().getName());
        }

        // test combination on multiple restrictions
        for (Map.Entry<PropertyState, RestrictionPattern> entry : map.entrySet()) {
            restrictions.setProperty(entry.getKey());
        }
        RestrictionPattern pattern = provider.getPattern("/testPath", restrictions);
        assertTrue(pattern instanceof CompositePattern);
    }

    @Test
    public void testGetPatternForAllSupported() throws Exception {
        Map<PropertyState, RestrictionPattern> map = new HashMap<>();
        String globRestriction = "/*/jcr:content";
        map.put(PropertyStates.createProperty(REP_GLOB, globRestriction), GlobPattern.create("/testPath", globRestriction));
        List<String> ntNames = ImmutableList.of(JcrConstants.NT_FOLDER, JcrConstants.NT_LINKEDFILE);
        map.put(PropertyStates.createProperty(REP_NT_NAMES, ntNames, Type.NAMES), new NodeTypePattern(ntNames));
        List<String> prefixes = ImmutableList.of("rep", "jcr");
        map.put(PropertyStates.createProperty(REP_PREFIXES, prefixes, Type.STRINGS), new PrefixPattern(prefixes));
        List<String> itemNames = ImmutableList.of("abc", "jcr:primaryType");
        map.put(PropertyStates.createProperty(REP_ITEM_NAMES, prefixes, Type.NAMES), new ItemNamePattern(itemNames));
        List<String> propNames = ImmutableList.of("jcr:mixinTypes", "jcr:primaryType");
        map.put(PropertyStates.createProperty(REP_CURRENT, propNames, Type.STRINGS), new CurrentPattern("/testPath", propNames));
        List<String> globs = Collections.singletonList(globRestriction);
        map.put(PropertyStates.createProperty(REP_GLOBS, globs, Type.STRINGS), new GlobsPattern("/testPath", globs));
        List<String> subtrees = ImmutableList.of("/sub/tree", "/a/b/c/");
        map.put(PropertyStates.createProperty(REP_SUBTREES, subtrees, Type.STRINGS), new SubtreePattern("/testPath", subtrees));

        Tree tree = TreeUtil.getOrAddChild(root.getTree("/"), "testPath", JcrConstants.NT_UNSTRUCTURED);
        Tree restrictions = TreeUtil.addChild(tree, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        for (Map.Entry<PropertyState, RestrictionPattern> entry : map.entrySet()) {
            restrictions.setProperty(entry.getKey());
        }

        RestrictionPattern pattern = provider.getPattern("/testPath", restrictions);
        assertTrue(pattern instanceof CompositePattern);
    }

    @Test
    public void testGetPatternFromRestrictions() throws Exception {
        String globRestriction = "/*/jcr:content";

        Map<PropertyState, RestrictionPattern> map = new HashMap<>();
        map.put(PropertyStates.createProperty(REP_GLOB, globRestriction), GlobPattern.create("/testPath", globRestriction));

        List<String> ntNames = ImmutableList.of(JcrConstants.NT_FOLDER, JcrConstants.NT_LINKEDFILE);
        map.put(PropertyStates.createProperty(REP_NT_NAMES, ntNames, Type.NAMES), new NodeTypePattern(ntNames));

        List<String> prefixes = ImmutableList.of("rep", "jcr");
        map.put(PropertyStates.createProperty(REP_PREFIXES, prefixes, Type.STRINGS), new PrefixPattern(prefixes));

        List<String> itemNames = ImmutableList.of("abc", "jcr:primaryType");
        map.put(PropertyStates.createProperty(REP_ITEM_NAMES, itemNames, Type.NAMES), new ItemNamePattern(itemNames));

        List<String> propNames = ImmutableList.of("*");
        map.put(PropertyStates.createProperty(REP_CURRENT, propNames, Type.STRINGS), new CurrentPattern("/testPath", propNames));

        List<String> globs = Collections.singletonList(globRestriction);
        map.put(PropertyStates.createProperty(REP_GLOBS, globs, Type.STRINGS), new GlobsPattern("/testPath", globs));

        List<String> subtrees = ImmutableList.of("/sub/tree", "/a/b/c/");
        map.put(PropertyStates.createProperty(REP_SUBTREES, subtrees, Type.STRINGS), new SubtreePattern("/testPath", subtrees));

        Tree tree = TreeUtil.getOrAddChild(root.getTree("/"), "testPath", JcrConstants.NT_UNSTRUCTURED);
        Tree restrictions = TreeUtil.addChild(tree, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);

        // test restrictions individually
        for (Map.Entry<PropertyState, RestrictionPattern> entry : map.entrySet()) {
            restrictions.setProperty(entry.getKey());

            RestrictionPattern pattern = provider.getPattern("/testPath", provider.readRestrictions("/testPath", tree));
            assertEquals(entry.getValue(), pattern);
            restrictions.removeProperty(entry.getKey().getName());
        }

        // test combination on multiple restrictions
        for (Map.Entry<PropertyState, RestrictionPattern> entry : map.entrySet()) {
            restrictions.setProperty(entry.getKey());
        }
        RestrictionPattern pattern = provider.getPattern("/testPath", provider.readRestrictions("/testPath", tree));
        assertTrue(pattern instanceof CompositePattern);
    }

    @Test
    public void testGetPatternFromInvalidRestrictionSet() {
        PropertyState ps = PropertyStates.createProperty("invalid", Collections.singleton("value"), Type.STRINGS);
        Set<Restriction> restrictions = Collections.singleton(new RestrictionImpl(ps, false));
        RestrictionPattern pattern = provider.getPattern("/testPath", restrictions);
        assertSame(RestrictionPattern.EMPTY, pattern);
    }

    @Test
    public void testGetPatternFromTreeNullPath() {
        assertSame(RestrictionPattern.EMPTY, provider.getPattern(null, mock(Tree.class)));
    }

    @Test
    public void testGetPatternFromRestrictionsNullPath() {
        assertSame(RestrictionPattern.EMPTY, provider.getPattern(null, Set.of(mock(Restriction.class))));
    }

    @Test
    public void testGetPatternFromEmptyRestrictions() {
        assertSame(RestrictionPattern.EMPTY, provider.getPattern("/testPath", Set.of()));
    }

    @Test(expected = AccessControlException.class)
    public void testValidateGlobRestriction() throws Exception {
        Tree t = TreeUtil.getOrAddChild(root.getTree("/"), "testTree", JcrConstants.NT_UNSTRUCTURED);
        String path = t.getPath();

        AccessControlManager acMgr = getAccessControlManager(root);

        List<String> globs = ImmutableList.of(
                "/1*/2*/3*/4*/5*/6*/7*/8*/9*/10*/11*/12*/13*/14*/15*/16*/17*/18*/19*/20*/21*",
                "*********************");
        for (String glob : globs) {
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
            acl.addEntry(getTestUser().getPrincipal(),
                    AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_READ),
                    true, Collections.singletonMap(REP_GLOB, getValueFactory().createValue(glob)));
            acMgr.setPolicy(path, acl);

            try {
                provider.validateRestrictions(path, t.getChild(REP_POLICY).getChild("allow"));
            } finally {
                acMgr.removePolicy(path, acl);
            }
        }
    }

    @Test(expected = AccessControlException.class)
    public void testValidateMvGlobRestriction() throws Exception {
        Tree t = TreeUtil.getOrAddChild(root.getTree("/"), "testTree", JcrConstants.NT_UNSTRUCTURED);
        String path = t.getPath();

        AccessControlManager acMgr = getAccessControlManager(root);

        ValueFactory vf = getValueFactory(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addEntry(getTestUser().getPrincipal(),
                AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_READ),
                true, Collections.emptyMap(), Collections.singletonMap(REP_GLOBS, new Value[] {
                        vf.createValue("/1*/2*/3*/4*/5*/6*/7*/8*/9*/10*/11*/12*/13*/14*/15*/16*/17*/18*/19*/20*/21*"),
                        vf.createValue("*********************")}));
        acMgr.setPolicy(path, acl);

        try {
            provider.validateRestrictions(path, t.getChild(REP_POLICY).getChild("allow"));
        } finally {
            acMgr.removePolicy(path, acl);
        }
    }
}