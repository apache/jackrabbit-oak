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

import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JcrAllTest extends AbstractSecurityTest implements PrivilegeConstants {

    private PrivilegeBitsProvider bitsProvider;

    @Override
    public void before() throws Exception {
        super.before();

        bitsProvider = new PrivilegeBitsProvider(root);
    }

    @Test
    public void testCalculatePermissionsAll() {
        PrivilegeBits all = bitsProvider.getBits(JCR_ALL);
        assertFalse(Permissions.ALL == PrivilegeBits.calculatePermissions(all, PrivilegeBits.EMPTY, true));
        assertTrue(Permissions.ALL == PrivilegeBits.calculatePermissions(all, all, true));
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