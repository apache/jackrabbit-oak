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

import java.util.Map;
import java.util.Set;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AbstractRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Additional tests for {@link AbstractRestrictionProvider} that require a full
 * oak repository.
 */
public class AbstractRestrictionProviderTest extends AbstractSecurityTest implements AccessControlConstants {

    private String testPath = "/testRoot";

    private Value globValue;
    private Value[] nameValues;

    private ValueFactory valueFactory;
    private AbstractRestrictionProvider restrictionProvider;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        valueFactory = new ValueFactoryImpl(root, namePathMapper);
        globValue = valueFactory.createValue("*");
        nameValues = new Value[] {
                valueFactory.createValue("nt:folder", PropertyType.NAME),
                valueFactory.createValue("nt:file", PropertyType.NAME)
        };

        RestrictionDefinition glob = new RestrictionDefinitionImpl(REP_GLOB, Type.STRING, false);
        RestrictionDefinition nts  = new RestrictionDefinitionImpl(REP_NT_NAMES, Type.NAMES, false);
        RestrictionDefinition mand = new RestrictionDefinitionImpl("mandatory", Type.BOOLEAN, true);
        Map<String, ? extends RestrictionDefinition> supported = ImmutableMap.of(glob.getName(), glob, nts.getName(), nts, mand.getName(), mand);
        restrictionProvider = new TestProvider(supported);
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    private Tree getAceTree(Restriction... restrictions) throws Exception {
        Tree rootNode = root.getTree("/");
        Tree tmp = TreeUtil.addChild(rootNode, "testRoot", JcrConstants.NT_UNSTRUCTURED);
        Tree policy = TreeUtil.addChild(tmp, REP_POLICY, NT_REP_ACL);
        Tree ace = TreeUtil.addChild(policy, "ace0", NT_REP_GRANT_ACE);
        restrictionProvider.writeRestrictions(tmp.getPath(), ace, ImmutableSet.copyOf(restrictions));
        return ace;
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
    public void testWriteRestrictions() throws Exception {
        Restriction r = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        Tree aceTree = getAceTree();

        restrictionProvider.writeRestrictions(testPath, aceTree, ImmutableSet.of(r));

        assertTrue(aceTree.hasChild(REP_RESTRICTIONS));
        Tree restr = aceTree.getChild(REP_RESTRICTIONS);
        assertEquals(r.getProperty(), restr.getProperty(REP_GLOB));
    }

    @Test
    public void testWriteInvalidRestrictions() throws Exception {
        PropertyState ps = PropertyStates.createProperty(REP_GLOB, valueFactory.createValue(false));
        Tree aceTree = getAceTree();

        restrictionProvider.writeRestrictions(testPath, aceTree, ImmutableSet.of(new RestrictionImpl(ps, false)));

        assertTrue(aceTree.hasChild(REP_RESTRICTIONS));
        Tree restr = aceTree.getChild(REP_RESTRICTIONS);
        assertEquals(ps, restr.getProperty(REP_GLOB));
    }

    @Test
    public void testValidateRestrictionsUnsupportedPath() throws Exception {
        // empty restrictions => must succeed
        restrictionProvider.validateRestrictions(null, getAceTree());


        // non-empty restrictions => must fail
        try {
            Restriction restr = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
            restrictionProvider.validateRestrictions(null, getAceTree(restr));
            fail();
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateRestrictionsWrongType() throws Exception {
        Restriction mand = restrictionProvider.createRestriction(testPath, "mandatory", valueFactory.createValue(true));
        try {
            Tree ace = getAceTree(mand);
            ace.getChild(REP_RESTRICTIONS).setProperty(REP_GLOB, true);

            restrictionProvider.validateRestrictions(testPath, ace);
            fail("wrong type with restriction 'rep:glob");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateRestrictionsUnsupportedRestriction() throws Exception {
        Restriction mand = restrictionProvider.createRestriction(testPath, "mandatory", valueFactory.createValue(true));
        try {
            Tree ace = getAceTree(mand);
            ace.getChild(REP_RESTRICTIONS).setProperty("Unsupported", "value");

            restrictionProvider.validateRestrictions(testPath, ace);
            fail("wrong type with restriction 'rep:glob");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testValidateRestrictionsMissingMandatory() throws Exception {
        Restriction glob = restrictionProvider.createRestriction(testPath, REP_GLOB, globValue);
        try {
            restrictionProvider.validateRestrictions(testPath, getAceTree(glob));
            fail("missing mandatory restriction");
        } catch (AccessControlException e) {
            // success
        }
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
}