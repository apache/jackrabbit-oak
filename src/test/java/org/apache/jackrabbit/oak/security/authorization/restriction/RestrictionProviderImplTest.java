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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlTest;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositePattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RestrictionProviderImpl}
 */
public class RestrictionProviderImplTest extends AbstractAccessControlTest implements AccessControlConstants {

    private RestrictionProviderImpl provider;

    @Before
    public void before() throws Exception {
        super.before();

        provider = new RestrictionProviderImpl();
    }

    @Test
    public void testGetSupportedDefinitions() {
        assertTrue(provider.getSupportedRestrictions(null).isEmpty());

        Set<RestrictionDefinition> defs = provider.getSupportedRestrictions("/testPath");
        assertNotNull(defs);
        assertEquals(2, defs.size());

        for (RestrictionDefinition def : defs) {
            if (REP_GLOB.equals(def.getName())) {
                assertEquals(Type.STRING, def.getRequiredType());
                assertFalse(def.isMandatory());
            } else if (REP_NT_NAMES.equals(def.getName())) {
                assertEquals(Type.NAMES, def.getRequiredType());
                assertFalse(def.isMandatory());
            } else {
                fail("unexpected restriction "+def.getName());
            }
        }
    }

    @Test
    public void testGetRestrictionPattern() {
        Map<PropertyState, RestrictionPattern> map = newHashMap();
        map.put(PropertyStates.createProperty(REP_GLOB, "/*/jcr:content"), GlobPattern.create("/testPath", "/*/jcr:content"));
        List<String> ntNames = ImmutableList.of(JcrConstants.NT_FOLDER, JcrConstants.NT_LINKEDFILE);
        map.put(PropertyStates.createProperty(REP_NT_NAMES, ntNames, Type.NAMES), new NodeTypePattern(ntNames));

        NodeUtil tree = new NodeUtil(root.getTree("/")).getOrAddTree("testPath", JcrConstants.NT_UNSTRUCTURED);
        Tree restrictions = tree.addChild("restrictions", NT_REP_RESTRICTIONS).getTree();

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
}