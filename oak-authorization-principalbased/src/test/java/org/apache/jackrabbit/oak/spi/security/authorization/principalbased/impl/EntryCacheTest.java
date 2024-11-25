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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.MIX_REP_PRINCIPAL_BASED_MIXIN;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_ENTRY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class EntryCacheTest extends AbstractPrincipalBasedTest {

    private RestrictionProvider restrictionProvider;
    private String accessControlledPath;
    private Tree policyTree;

    @Before
    public void before() throws Exception {
        super.before();

        restrictionProvider = spy(getConfig(AuthorizationConfiguration.class).getRestrictionProvider());
        accessControlledPath = getNamePathMapper().getOakPath(getTestSystemUser().getPath());
        Tree accessControlledTree = root.getTree(accessControlledPath);
        TreeUtil.addMixin(accessControlledTree, MIX_REP_PRINCIPAL_BASED_MIXIN, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");
        policyTree = TreeUtil.addChild(accessControlledTree, REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY);
    }
    
    @After
    public void after() throws Exception {
        try {
            reset(restrictionProvider);
        } finally {
            super.after();
        }
    }

    @Test
    public void testNoEntries() {
        EntryCache cache = new EntryCache(root, Set.of(accessControlledPath), restrictionProvider);
        assertFalse(cache.getEntries(TEST_OAK_PATH).hasNext());
    }

    @Test
    public void testNonEntryChild() throws Exception {
        TreeUtil.addChild(policyTree, "invalidChild", NT_OAK_UNSTRUCTURED);
        EntryCache cache = new EntryCache(root, Set.of(accessControlledPath), restrictionProvider);
        assertFalse(cache.getEntries(TEST_OAK_PATH).hasNext());
    }

    @Test
    public void testMissingEntriesForTestPath() throws Exception {
        Tree entry = TreeUtil.addChild(policyTree, "entry1", NT_REP_PRINCIPAL_ENTRY);
        entry.setProperty(REP_EFFECTIVE_PATH, PathUtils.ROOT_PATH, Type.PATH);
        entry.setProperty(REP_PRIVILEGES, Set.of(JCR_READ), Type.NAMES);

        EntryCache cache = new EntryCache(root, Set.of(accessControlledPath), restrictionProvider);
        assertFalse(cache.getEntries(TEST_OAK_PATH).hasNext());
    }

    @Test
    public void testEntriesForTestPath() throws Exception {
        Tree entry = TreeUtil.addChild(policyTree, "entry1", NT_REP_PRINCIPAL_ENTRY);
        entry.setProperty(REP_EFFECTIVE_PATH, TEST_OAK_PATH, Type.PATH);
        entry.setProperty(REP_PRIVILEGES, Set.of(JCR_READ), Type.NAMES);

        EntryCache cache = new EntryCache(root, Set.of(accessControlledPath), restrictionProvider);
        assertTrue(cache.getEntries(TEST_OAK_PATH).hasNext());
        verifyNoInteractions(restrictionProvider);
    }

    @Test
    public void testEntriesWithRestrictionsForTestPath() throws Exception {
        Tree entry = TreeUtil.addChild(policyTree, "entry1", NT_REP_PRINCIPAL_ENTRY);
        entry.setProperty(REP_EFFECTIVE_PATH, TEST_OAK_PATH, Type.PATH);
        entry.setProperty(REP_PRIVILEGES, Set.of(JCR_READ), Type.NAMES);
        restrictionProvider.writeRestrictions(TEST_OAK_PATH, entry, 
                Collections.singleton(restrictionProvider.createRestriction(TEST_OAK_PATH, REP_GLOB, getValueFactory(root).createValue("test"))));

        EntryCache cache = new EntryCache(root, Set.of(accessControlledPath), restrictionProvider);
        assertTrue(cache.getEntries(TEST_OAK_PATH).hasNext());
        
        verify(restrictionProvider).readRestrictions(eq(TEST_OAK_PATH), any(Tree.class));
        verify(restrictionProvider).getPattern(eq(TEST_OAK_PATH), any(Set.class));
    }
}