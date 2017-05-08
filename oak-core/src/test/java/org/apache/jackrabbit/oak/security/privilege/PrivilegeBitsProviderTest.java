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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.Set;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Additional tests for PrivilegeBitsProvider based on the default privileges
 * installed by {@link PrivilegeInitializer} during the default security setup.
 */
public class PrivilegeBitsProviderTest extends AbstractSecurityTest implements PrivilegeConstants {

    private PrivilegeBitsProvider bitsProvider;

    @Override
    public void before() throws Exception {
        super.before();

        bitsProvider = new PrivilegeBitsProvider(root);
    }

    @Test
    public void testGetPrivilegesTree() {
        assertEquals(PRIVILEGES_PATH, bitsProvider.getPrivilegesTree().getPath());
    }

    @Test
    public void testGetBitsFromInvalidPrivilege() {
        assertEquals(PrivilegeBits.EMPTY, bitsProvider.getBits("invalid1", "invalid2"));
    }

    @Test
    public void testGetPrivilegeNames() throws RepositoryException {
        PrivilegeBits bits = bitsProvider.getBits(JCR_READ_ACCESS_CONTROL);
        Set<String> names = bitsProvider.getPrivilegeNames(bits);

        assertEquals(1, names.size());
        assertEquals(JCR_READ_ACCESS_CONTROL, names.iterator().next());
    }

    @Test
    public void testAggregation() throws RepositoryException {
        PrivilegeBits writeBits = bitsProvider.getBits(JCR_ADD_CHILD_NODES,
                JCR_REMOVE_CHILD_NODES,
                JCR_REMOVE_NODE,
                JCR_MODIFY_PROPERTIES);
        Set<String> names = bitsProvider.getPrivilegeNames(writeBits);
        assertEquals(1, names.size());
        assertEquals(JCR_WRITE, names.iterator().next());
    }

    @Test
    public void testUnknownAggregation() throws RepositoryException {
        PrivilegeBits bits = bitsProvider.getBits(REP_WRITE, JCR_LIFECYCLE_MANAGEMENT);
        Set<String> names = bitsProvider.getPrivilegeNames(bits);

        assertEquals(2, names.size());
    }

    @Test
    public void testRedundantAggregation() throws RepositoryException {
        PrivilegeBits writeBits = bitsProvider.getBits(REP_WRITE);
        Set<String> names = bitsProvider.getPrivilegeNames(writeBits);

        assertEquals(1, names.size());
        assertEquals(REP_WRITE, names.iterator().next());

        writeBits = bitsProvider.getBits(REP_WRITE, JCR_WRITE);
        names = bitsProvider.getPrivilegeNames(writeBits);

        assertEquals(1, names.size());
        assertEquals(REP_WRITE, names.iterator().next());
    }

    @Test
    public void testGetAggregatedNamesUnknown() throws Exception {
        assertFalse(bitsProvider.getAggregatedPrivilegeNames("unknown").iterator().hasNext());
    }

    @Test
    public void testGetAggregatedNamesJcrAll() throws Exception {
        assertEquals(NON_AGGREGATE_PRIVILEGES, ImmutableSet.copyOf(bitsProvider.getAggregatedPrivilegeNames(JCR_ALL)));
    }

    @Test
    public void testGetAggregatedNamesIncludingJcrAll() throws Exception {
        assertEquals(NON_AGGREGATE_PRIVILEGES, ImmutableSet.copyOf(bitsProvider.getAggregatedPrivilegeNames(JCR_READ, JCR_WRITE, JCR_ALL)));
    }

    @Test
    public void getAggregatedNamesWithCustom() throws Exception {
        PrivilegeManager pMgr = getPrivilegeManager(root);
        pMgr.registerPrivilege("test1", true, null);

        assertEquals(ImmutableSet.of("test1"), ImmutableSet.copyOf(bitsProvider.getAggregatedPrivilegeNames("test1")));

        Set<String> expected = Sets.newHashSet(NON_AGGREGATE_PRIVILEGES);
        expected.add("test1");
        assertEquals(expected, ImmutableSet.copyOf(bitsProvider.getAggregatedPrivilegeNames(JCR_ALL)));
        assertEquals(expected, ImmutableSet.copyOf(bitsProvider.getAggregatedPrivilegeNames(JCR_ALL)));

    }
}