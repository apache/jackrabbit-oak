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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_POLICY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemappedPrivilegeNamesTest extends AbstractAccessControlTest {

    private static final Map<String, String> LOCAL_NAME_MAPPINGS = ImmutableMap.of(
            "a","internal",
            "b","http://www.jcp.org/jcr/1.0",
            "c","http://jackrabbit.apache.org/oak/ns/1.0"
    );

    private NamePathMapperImpl remapped;

    @Override
    protected NamePathMapper getNamePathMapper() {
        if (remapped == null) {
            remapped = new NamePathMapperImpl(new LocalNameMapper(root, LOCAL_NAME_MAPPINGS));
        }
        return remapped;
    }

    protected Privilege[] privilegesFromNames(@NotNull String... privilegeNames) throws RepositoryException {
        Iterable<String> jcrNames = Iterables.transform(Arrays.asList(privilegeNames), s -> getNamePathMapper().getJcrName(s));
        return super.privilegesFromNames(jcrNames);
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryWithOakPrivilegeName() throws Exception {
        Privilege[] privs = new Privilege[] {when(mock(Privilege.class).getName()).thenReturn(PrivilegeConstants.JCR_READ).getMock()};
        acl.addAccessControlEntry(testPrincipal, privs);
    }

    @Test
    public void testAddEntryWithJcrPrivilegeName() throws Exception {
        assertTrue(acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_READ)));
        List<ACE> entries = acl.getEntries();
        assertEquals(1, entries.size());
        assertEquals(getBitsProvider().getBits(PrivilegeConstants.JCR_READ), entries.get(0).getPrivilegeBits());
    }

    @Test
    public void testWriteEntryWithJcrPrivilegeName() throws Exception {
        assertTrue(acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_READ)));

        getAccessControlManager(root).setPolicy(acl.getPath(), acl);
        Tree aceTree = root.getTree(acl.getPath()).getChild(REP_POLICY).getChildren().iterator().next();
        Iterable<String> privNames = TreeUtil.getNames(aceTree, REP_PRIVILEGES);
        assertTrue(Iterables.elementsEqual(ImmutableList.of(PrivilegeConstants.JCR_READ), privNames));
    }
}