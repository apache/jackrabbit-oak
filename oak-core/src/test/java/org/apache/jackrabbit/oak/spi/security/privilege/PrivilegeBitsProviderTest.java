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
package org.apache.jackrabbit.oak.spi.security.privilege;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class PrivilegeBitsProviderTest extends AbstractSecurityTest implements PrivilegeConstants {

    private PrivilegeBitsProvider bitsProvider;

    @Override
    public void before() throws Exception {
        super.before();

        bitsProvider = new PrivilegeBitsProvider(root);
    }

    @Test
    public void testGetPrivilegesTree() {
        assertNotNull(bitsProvider.getPrivilegesTree());
        assertEquals(PRIVILEGES_PATH, bitsProvider.getPrivilegesTree().getPath());
    }

    @Test
    public void testGetBits() {
        PrivilegeBits bits = bitsProvider.getBits(JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES);
        assertFalse(bits.isEmpty());

        PrivilegeBits mod = PrivilegeBits.getInstance(bitsProvider.getBits(JCR_ADD_CHILD_NODES)).add(bitsProvider.getBits(JCR_REMOVE_CHILD_NODES));
        assertEquals(bits, mod.unmodifiable());
    }

    @Test
    public void testGetBitsFromInvalidPrivilege() {
        assertEquals(PrivilegeBits.EMPTY, bitsProvider.getBits("invalid1", "invalid2"));
    }

    @Test
    public void testGetBitsFromEmpty() {
        assertEquals(PrivilegeBits.EMPTY, bitsProvider.getBits());
        assertEquals(PrivilegeBits.EMPTY, bitsProvider.getBits(new String[0]));
        assertEquals(PrivilegeBits.EMPTY, bitsProvider.getBits(""));
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
    public void testAll() {
        PrivilegeBits all = bitsProvider.getBits(JCR_ALL);
        assertFalse(all.isEmpty());
        assertEquals(Collections.singleton(JCR_ALL), bitsProvider.getPrivilegeNames(all));
    }

    @Test
    public void testAllAggregation() throws Exception {
        PrivilegeBits all = bitsProvider.getBits(JCR_ALL);

        PrivilegeManager pMgr = getSecurityProvider().getConfiguration(PrivilegeConfiguration.class).getPrivilegeManager(root, NamePathMapper.DEFAULT);
        Iterable<Privilege> declaredAggr = Arrays.asList(pMgr.getPrivilege(JCR_ALL).getDeclaredAggregatePrivileges());
        String[] allAggregates = Iterables.toArray(Iterables.transform(
                declaredAggr,
                new Function<Privilege, String>() {
                    @Override
                    public String apply(@Nullable Privilege privilege) {
                        return checkNotNull(privilege).getName();
                    }
                }), String.class);
        PrivilegeBits all2 = bitsProvider.getBits(allAggregates);

        assertEquals(all, all2);
        assertEquals(Collections.singleton(JCR_ALL), bitsProvider.getPrivilegeNames(all2));

        PrivilegeBits bits = PrivilegeBits.getInstance();
        for (String name : allAggregates) {
            bits.add(bitsProvider.getBits(name));
        }
        assertEquals(all, bits.unmodifiable());
    }
}