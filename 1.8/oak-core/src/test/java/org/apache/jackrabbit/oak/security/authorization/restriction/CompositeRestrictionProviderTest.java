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

import java.util.Set;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositePattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Additional tests for {@link CompositeRestrictionProvider} that require a full
 * oak repository.
 */
public class CompositeRestrictionProviderTest extends AbstractSecurityTest implements AccessControlConstants {

    private RestrictionProvider rp1 = new TestProvider(ImmutableMap.<String, RestrictionDefinition>of(
            REP_GLOB, new RestrictionDefinitionImpl(REP_GLOB, Type.STRING, false),
            REP_NT_NAMES, new RestrictionDefinitionImpl(REP_NT_NAMES, Type.NAMES, false),
            REP_PREFIXES, new RestrictionDefinitionImpl(REP_PREFIXES, Type.STRINGS, false)
    ));
    private RestrictionProvider rp2 = new TestProvider(ImmutableMap.of(
            "boolean", new RestrictionDefinitionImpl("boolean", Type.BOOLEAN, true),
            "longs", new RestrictionDefinitionImpl("longs", Type.LONGS, false)
    ));

    private RestrictionProvider rp3 = new TestProvider(ImmutableMap.of(
            "string", new RestrictionDefinitionImpl("string", Type.STRING, false)),
            true
    );

    private Set<String> supported = ImmutableSet.of("boolean", "longs", REP_NT_NAMES, REP_GLOB);
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
        NodeUtil aceNode = new NodeUtil(root.getTree("/")).addChild("test", NT_REP_GRANT_ACE);
        aceNode.setBoolean("boolean", true);
        aceNode.setValues("longs", new Value[] {vf.createValue(10), vf.createValue(290)});
        aceNode.setString(REP_GLOB, "*");
        aceNode.setNames(REP_NT_NAMES); // empty array
        aceNode.setString("invalid", "val");
        aceNode.setStrings("invalid2", "val1", "val2", "val3");

        Set<Restriction> restrictions = provider.readRestrictions("/test", aceNode.getTree());
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
        NodeUtil aceNode = new NodeUtil(root.getTree("/")).addChild("test", NT_REP_GRANT_ACE);
        Set<Restriction> restrictions = ImmutableSet.of(
                provider.createRestriction("/test","boolean", vf.createValue(true)),
                provider.createRestriction("/test", "longs"),
                provider.createRestriction("/test", REP_GLOB, vf.createValue("*")),
                provider.createRestriction("/test", REP_NT_NAMES, vf.createValue("nt:base", PropertyType.NAME), vf.createValue("nt:version", PropertyType.NAME)));
        provider.writeRestrictions("/test", aceNode.getTree(), restrictions);
    }

    @Test
    public void testWriteUnsupportedRestrictions() throws Exception {
        NodeUtil aceNode = new NodeUtil(root.getTree("/")).addChild("test", NT_REP_GRANT_ACE);
        Restriction invalid = new RestrictionImpl(PropertyStates.createProperty("invalid", vf.createValue(true)), false);
        try {
            provider.writeRestrictions("/test", aceNode.getTree(), ImmutableSet.<Restriction>of(invalid));
            fail("AccessControlException expected");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateRestrictions() throws Exception {
        NodeUtil aceNode = new NodeUtil(root.getTree("/")).addChild("test", NT_REP_GRANT_ACE);
        NodeUtil rNode = aceNode.addChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rNode.setBoolean("boolean", true);
        rNode.setValues("longs", new Value[] {vf.createValue(10), vf.createValue(290)});
        rNode.setString(REP_GLOB, "*");
        rNode.setNames(REP_NT_NAMES); // empty array

        provider.validateRestrictions("/test", aceNode.getTree());

        // remove mandatory restriction
        rNode.removeProperty("boolean");
        try {
            provider.validateRestrictions("/test", aceNode.getTree());
            fail("validation should detect missing mandatory restrictions");
        } catch (AccessControlException e) {
            // success
        }

        // set with wrong type
        rNode.setName("boolean", "nt:base");
        try {
            provider.validateRestrictions("/test", aceNode.getTree());
            fail("validation should detect wrong restriction type");
        } catch (AccessControlException e) {
            // success
        } finally {
            rNode.setBoolean("boolean", true);
        }

        rNode.setStrings(REP_GLOB, "*", "/jcr:content");
        try {
            provider.validateRestrictions("/test", aceNode.getTree());
            fail("validation should detect wrong restriction type (multi vs single valued)");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateRestrictionsAtEntryNode() throws Exception {
        NodeUtil aceNode = new NodeUtil(root.getTree("/")).addChild("test", NT_REP_GRANT_ACE);
        aceNode.setBoolean("boolean", true);
        aceNode.setValues("longs", new Value[] {vf.createValue(10), vf.createValue(290)});
        aceNode.setString(REP_GLOB, "*");
        aceNode.setNames(REP_NT_NAMES); // empty array

        provider.validateRestrictions("/test", aceNode.getTree());
    }

    @Test
    public void testValidateInvalidRestrictionDef() throws Exception {
        RestrictionProvider rp = CompositeRestrictionProvider.newInstance(rp1, rp3);

        NodeUtil aceNode = new NodeUtil(root.getTree("/")).addChild("test", NT_REP_GRANT_ACE);
        NodeUtil rNode = aceNode.addChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rNode.setValues(REP_GLOB, new Value[]{vf.createValue(10), vf.createValue(290)});

        try {
            rp.validateRestrictions("/test", aceNode.getTree());
            fail("Validation must detect invalid restriction definition");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateUnsupportedRestriction() throws Exception {
        RestrictionProvider rp = CompositeRestrictionProvider.newInstance(rp1, rp3);

        NodeUtil aceNode = new NodeUtil(root.getTree("/")).addChild("test", NT_REP_GRANT_ACE);
        NodeUtil rNode = aceNode.addChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rNode.setString("unsupported", "value");

        try {
            rp.validateRestrictions("/test", aceNode.getTree());
            fail("Validation must detect unsupported restriction");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetRestrictionPattern() throws Exception {
        NodeUtil aceNode = new NodeUtil(root.getTree("/")).addChild("test", NT_REP_GRANT_ACE);
        NodeUtil rNode = aceNode.addChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rNode.setString(REP_GLOB, "*");

        assertFalse(provider.getPattern("/test", aceNode.getTree()) instanceof CompositePattern);

        rNode.setBoolean("boolean", true);
        rNode.setValues("longs", new Value[]{vf.createValue(10), vf.createValue(290)});

        assertTrue(provider.getPattern("/test", rNode.getTree()) instanceof CompositePattern);
    }
}