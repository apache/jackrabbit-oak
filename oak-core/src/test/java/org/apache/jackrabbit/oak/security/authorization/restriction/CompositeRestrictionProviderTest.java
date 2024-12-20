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
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositePattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Additional tests for {@link CompositeRestrictionProvider} that require a full
 * oak repository.
 */
public class CompositeRestrictionProviderTest extends AbstractSecurityTest implements AccessControlConstants {

    private RestrictionProvider rp1 = new TestProvider(Map.of(
            REP_GLOB, new RestrictionDefinitionImpl(REP_GLOB, Type.STRING, false),
            REP_NT_NAMES, new RestrictionDefinitionImpl(REP_NT_NAMES, Type.NAMES, false),
            REP_PREFIXES, new RestrictionDefinitionImpl(REP_PREFIXES, Type.STRINGS, false)
    ));
    private RestrictionProvider rp2 = new TestProvider(Map.of(
            "boolean", new RestrictionDefinitionImpl("boolean", Type.BOOLEAN, true),
            "longs", new RestrictionDefinitionImpl("longs", Type.LONGS, false)
    ));

    private RestrictionProvider rp3 = new TestProvider(Map.of(
            "string", new RestrictionDefinitionImpl("string", Type.STRING, false)),
            true
    );

    private Set<String> supported = Set.of("boolean", "longs", REP_NT_NAMES, REP_GLOB);
    private RestrictionProvider provider = CompositeRestrictionProvider.newInstance(rp1, rp2);

    private ValueFactory vf;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        vf = getValueFactory();
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @Test
    public void testReadRestrictions() throws Exception {
        Tree aceNode = TreeUtil.addChild(root.getTree("/"), "test", NT_REP_GRANT_ACE);
        aceNode.setProperty("boolean", true);
        aceNode.setProperty(PropertyStates.createProperty("longs", ImmutableList.of(vf.createValue(10), vf.createValue(290))));
        aceNode.setProperty(REP_GLOB, "*");
        aceNode.setProperty(REP_NT_NAMES, Set.of(), Type.NAMES); // empty array
        aceNode.setProperty("invalid", "val");
        aceNode.setProperty("invalid2", ImmutableList.of("val1", "val2", "val3"), Type.STRINGS);

        Set<Restriction> restrictions = provider.readRestrictions("/test", aceNode);
        assertEquals(4, restrictions.size());
        for (Restriction r : restrictions) {
            String name = r.getDefinition().getName();
            if (!supported.contains(name)) {
                fail("read unsupported restriction");
            }
        }
    }

    @Test
    public void testWriteRestrictions() throws Exception {
        Tree aceNode = TreeUtil.addChild(root.getTree("/"), "test", NT_REP_GRANT_ACE);
        Set<Restriction> restrictions = Set.of(
                provider.createRestriction("/test","boolean", vf.createValue(true)),
                provider.createRestriction("/test", "longs"),
                provider.createRestriction("/test", REP_GLOB, vf.createValue("*")),
                provider.createRestriction("/test", REP_NT_NAMES, vf.createValue("nt:base", PropertyType.NAME), vf.createValue("nt:version", PropertyType.NAME)));
        provider.writeRestrictions("/test", aceNode, restrictions);
    }

    @Test
    public void testWriteUnsupportedRestrictions() throws Exception {
        Tree aceNode = TreeUtil.addChild(root.getTree("/"), "test", NT_REP_GRANT_ACE);
        Restriction invalid = new RestrictionImpl(PropertyStates.createProperty("invalid", vf.createValue(true)), false);
        try {
            provider.writeRestrictions("/test", aceNode, Set.of(invalid));
            fail("AccessControlException expected");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateRestrictions() throws Exception {
        Tree aceNode = TreeUtil.addChild(root.getTree("/"), "test", NT_REP_GRANT_ACE);
        Tree rNode = TreeUtil.addChild(aceNode, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rNode.setProperty("boolean", true);
        rNode.setProperty(PropertyStates.createProperty("longs", ImmutableList.of(vf.createValue(10), vf.createValue(290))));
        rNode.setProperty(REP_GLOB, "*");
        rNode.setProperty(REP_NT_NAMES, ImmutableList.of(), Type.NAMES); // empty array

        provider.validateRestrictions("/test", aceNode);

        // remove mandatory restriction
        rNode.removeProperty("boolean");
        try {
            provider.validateRestrictions("/test", aceNode);
            fail("validation should detect missing mandatory restrictions");
        } catch (AccessControlException e) {
            // success
        }

        // set with wrong type
        rNode.setProperty("boolean", "nt:base", Type.NAME);
        try {
            provider.validateRestrictions("/test", aceNode);
            fail("validation should detect wrong restriction type");
        } catch (AccessControlException e) {
            // success
        } finally {
            rNode.setProperty("boolean", true);
        }

        rNode.setProperty(REP_GLOB, ImmutableList.of("*", "/jcr:content"), Type.STRINGS);
        try {
            provider.validateRestrictions("/test", aceNode);
            fail("validation should detect wrong restriction type (multi vs single valued)");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateRestrictionsAtEntryNode() throws Exception {
        Tree aceNode = TreeUtil.addChild(root.getTree("/"), "test", NT_REP_GRANT_ACE);
        aceNode.setProperty("boolean", true);
        aceNode.setProperty(PropertyStates.createProperty("longs", ImmutableList.of(vf.createValue(10), vf.createValue(290))));
        aceNode.setProperty(REP_GLOB, "*");
        aceNode.setProperty(REP_NT_NAMES, ImmutableList.of(), Type.NAMES); // empty array

        provider.validateRestrictions("/test", aceNode);
    }

    @Test
    public void testValidateInvalidRestrictionDef() throws Exception {
        RestrictionProvider rp = CompositeRestrictionProvider.newInstance(rp1, rp3);

        Tree aceNode = TreeUtil.addChild(root.getTree("/"), "test", NT_REP_GRANT_ACE);
        Tree rNode = TreeUtil.addChild(aceNode, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rNode.setProperty(PropertyStates.createProperty("longs", ImmutableList.of(vf.createValue(10), vf.createValue(290))));

        try {
            rp.validateRestrictions("/test", aceNode);
            fail("Validation must detect invalid restriction definition");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateUnsupportedRestriction() throws Exception {
        RestrictionProvider rp = CompositeRestrictionProvider.newInstance(rp1, rp3);

        Tree aceNode = TreeUtil.addChild(root.getTree("/"), "test", NT_REP_GRANT_ACE);
        Tree rNode = TreeUtil.addChild(aceNode, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rNode.setProperty("unsupported", "value");

        try {
            rp.validateRestrictions("/test", aceNode);
            fail("Validation must detect unsupported restriction");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetRestrictionPattern() throws Exception {
        Tree aceNode = TreeUtil.addChild(root.getTree("/"), "test", NT_REP_GRANT_ACE);
        Tree rNode = TreeUtil.addChild(aceNode, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rNode.setProperty(REP_GLOB, "*");

        assertFalse(provider.getPattern("/test", aceNode) instanceof CompositePattern);

        rNode.setProperty("boolean", true);
        rNode.setProperty(PropertyStates.createProperty("longs", ImmutableList.of(vf.createValue(10), vf.createValue(290))));

        assertTrue(provider.getPattern("/test", rNode) instanceof CompositePattern);
    }
}