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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EffectivePoliciesByPrincipalsAndPathsTest extends AbstractAccessControlTest {
    
    private static final String EXISTING_CHILD_PATH = TEST_PATH + "/child";
    private static final String NON_EXISTING_CHILD_PATH = TEST_PATH + "/child2";
    
    private AccessControlManagerImpl acMgr;
    
    @Override
    @Before
    public void before() throws Exception {
        super.before();

        acMgr = new AccessControlManagerImpl(root, NamePathMapper.DEFAULT, getSecurityProvider());
        
        testPrivileges = privilegesFromNames(JCR_READ);
        testPrincipal = getTestUser().getPrincipal();

        Tree t = root.getTree(TEST_PATH);
        Tree child = TreeUtil.addChild(t, "child", JcrConstants.NT_UNSTRUCTURED);

        ValueFactory vf = getValueFactory(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, t.getPath());
        acl.addEntry(testPrincipal, testPrivileges, true, Collections.emptyMap(), 
                Collections.singletonMap(AccessControlConstants.REP_SUBTREES, new Value[] {vf.createValue("child"), vf.createValue("child2")}));
        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false);
        acMgr.setPolicy(acl.getPath(), acl);

        acl = AccessControlUtils.getAccessControlList(acMgr, null);
        acl.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT), false);
        acMgr.setPolicy(acl.getPath(), acl);
        
        root.commit();
    }

    @After
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree(TEST_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetPoliciesEmptyPrincipalSet() throws Exception {
        assertFalse(acMgr.getEffectivePolicies(Collections.emptySet(), TEST_PATH).hasNext());
    }

    @Test(expected = AccessControlException.class)
    public void testGetPoliciesInvalidPrincipal() throws Exception {
        acMgr.getEffectivePolicies(Collections.singleton(() -> "non-existing"), TEST_PATH);
    }
    
    @Test
    public void testMissingPaths() throws Exception {
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(Collections.singleton(testPrincipal), new String[0]);
        AccessControlPolicy[] expected = acMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        
        assertArrayEquals(expected, Iterators.toArray(effective, AccessControlPolicy.class));
    }
    
    @Test
    public void testNullPath() throws Exception {
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(Collections.singleton(testPrincipal), new String[] {null});
        assertFalse(effective.hasNext());

        effective = acMgr.getEffectivePolicies(Collections.singleton(EveryonePrincipal.getInstance()), new String[] {null});
        assertEquals(1, Iterators.size(effective));
    }

    @Test
    public void testIncludingNullPath() throws Exception {
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(Collections.singleton(EveryonePrincipal.getInstance()), null, TEST_PATH);
        assertEquals(2, Iterators.size(effective));
    }
    
    @Test
    public void testNonExistingMatchingNodePath() throws Exception {
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(Collections.singleton(testPrincipal), NON_EXISTING_CHILD_PATH);
        assertEquals(1, Iterators.size(effective));

        effective = acMgr.getEffectivePolicies(Collections.singleton(EveryonePrincipal.getInstance()), NON_EXISTING_CHILD_PATH);
        assertEquals(1, Iterators.size(effective));

        // AC setup for both principals is on the same node -> only one effective policy expected
        effective = acMgr.getEffectivePolicies(Set.of(EveryonePrincipal.getInstance(), testPrincipal), NON_EXISTING_CHILD_PATH);
        assertEquals(1, Iterators.size(effective));
    }

    @Test
    public void testNonExistingNotMatchingNodePath() throws Exception {
        String nonMatchingPath = "/any/path";
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(Collections.singleton(testPrincipal), nonMatchingPath);
        assertFalse(effective.hasNext());

        effective = acMgr.getEffectivePolicies(Collections.singleton(EveryonePrincipal.getInstance()), nonMatchingPath);
        assertFalse(effective.hasNext());
    }
    
    @Test
    public void testNodePaths() throws Exception {
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(Collections.singleton(EveryonePrincipal.getInstance()), TEST_PATH);
        assertEquals(1, Iterators.size(effective));
    }

    @Test
    public void testPropertyPath() throws Exception {
        // prop path matching policy
        String propPath = PathUtils.concat(TEST_PATH, JCR_PRIMARYTYPE);
        
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(Collections.singleton(EveryonePrincipal.getInstance()), propPath);
        assertEquals(1, Iterators.size(effective));
        
        // no matching policy 
        effective = acMgr.getEffectivePolicies(Collections.singleton(testPrincipal), PathUtils.concat(TEST_PATH, JCR_PRIMARYTYPE));
        
    }
    
    @Test
    public void testRestrictions() throws Exception {
        Set<Principal> principalSet = Collections.singleton(testPrincipal);
        // ace for test-principal only takes effect in subtree
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(principalSet, TEST_PATH);
        assertFalse(effective.hasNext());

        // node paths
        effective = acMgr.getEffectivePolicies(principalSet, EXISTING_CHILD_PATH);
        assertEquals(1, Iterators.size(effective));

        effective = acMgr.getEffectivePolicies(principalSet, NON_EXISTING_CHILD_PATH);
        assertEquals(1, Iterators.size(effective));

        // property path
        String propPath = PathUtils.concat(EXISTING_CHILD_PATH, JCR_PRIMARYTYPE);
        effective = acMgr.getEffectivePolicies(principalSet, propPath);
        assertEquals(1, Iterators.size(effective));

        // non-matching path
        effective = acMgr.getEffectivePolicies(principalSet, TEST_PATH + "/non-matching");
        assertFalse(effective.hasNext());
    }
    
    @Test
    public void testReadablePaths() throws Exception {
        Set<Principal> principalSet = Set.of(testPrincipal, EveryonePrincipal.getInstance());

        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(principalSet, 
                NodeTypeConstants.NODE_TYPES_PATH, 
                NamespaceConstants.NAMESPACES_PATH);
        assertEquals(1, Iterators.size(effective));
    }
}