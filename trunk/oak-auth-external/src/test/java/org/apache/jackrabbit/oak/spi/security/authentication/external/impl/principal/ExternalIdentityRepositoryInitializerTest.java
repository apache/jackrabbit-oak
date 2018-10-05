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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExternalIdentityRepositoryInitializerTest extends AbstractExternalAuthTest {

    @Test
    public void testExternalIdIndexDefinition() throws Exception {
        Tree oakIndex = root.getTree('/' + IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(oakIndex.exists());

        Tree externalIdIndex = oakIndex.getChild("externalId");
        assertIndexDefinition(externalIdIndex, ExternalIdentityConstants.REP_EXTERNAL_ID, true);
    }

    @Test
    public void testPrincipalNamesIndexDefinition() throws Exception {
        Tree oakIndex = root.getTree('/' + IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(oakIndex.exists());

        Tree externalPrincipalNames = oakIndex.getChild("externalPrincipalNames");
        assertIndexDefinition(externalPrincipalNames, ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, false);
    }

    private static void assertIndexDefinition(Tree tree, String propName, boolean isUnique) {
        assertTrue(tree.exists());
        assertEquals(isUnique, TreeUtil.getBoolean(tree, IndexConstants.UNIQUE_PROPERTY_NAME));
        assertArrayEquals(
                propName, new String[]{propName},
                Iterables.toArray(TreeUtil.getStrings(tree, IndexConstants.PROPERTY_NAMES), String.class));
        Iterable<String> declaringNtNames = TreeUtil.getStrings(tree, IndexConstants.DECLARING_NODE_TYPES);
        assertNull(declaringNtNames);
    }
}