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

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.Privilege;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_ITEM_NAMES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemappedRestrictionNamesTest extends AbstractAccessControlTest {

    private static final Map<String, String> LOCAL_NAME_MAPPINGS = Map.of(
            "a","internal",
            "b","http://www.jcp.org/jcr/1.0",
            "c","http://jackrabbit.apache.org/oak/ns/1.0"
    );

    private NamePathMapperImpl remapped;

    private Privilege[] privs;
    private ACL acl;

    @Override
    public void before() throws Exception {
        super.before();

        privs = privilegesFromNames(PrivilegeConstants.JCR_READ);
        acl = createACL(TEST_PATH, Collections.emptyList(), getNamePathMapper(), getRestrictionProvider());
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        if (remapped == null) {
            remapped = new NamePathMapperImpl(new LocalNameMapper(root, LOCAL_NAME_MAPPINGS));
        }
        return remapped;
    }

    protected Privilege[] privilegesFromNames(@NotNull String... privilegeNames) throws RepositoryException {
        Iterable<String> jcrNames = Arrays.stream(privilegeNames).map(s -> getNamePathMapper().getJcrName(s)).collect(Collectors.toList());
        return super.privilegesFromNames(jcrNames);
    }

    @Test
    public void testAddEntryWithSingleValueRestriction() throws Exception {
        String jcrGlobName = getNamePathMapper().getJcrName(REP_GLOB);
        Map<String, Value> rest = Map.of(jcrGlobName, getValueFactory(root).createValue("*"));
        assertTrue(acl.addEntry(testPrincipal, privs, false, rest));

        List<ACE> entries = acl.getEntries();
        assertEquals(1, entries.size());
        assertArrayEquals(new String[] {jcrGlobName}, entries.get(0).getRestrictionNames());
        assertEquals(rest.get(jcrGlobName), entries.get(0).getRestriction(jcrGlobName));
    }

    @Test
    public void testAddEntryWithMVRestriction() throws Exception {
        String jcrItemNames = getNamePathMapper().getJcrName(REP_ITEM_NAMES);
        Value[] valArray = new Value[] {getValueFactory(root).createValue("myItemName", PropertyType.NAME)};
        Map<String, Value[]> rest = Map.of(jcrItemNames, valArray);
        assertTrue(acl.addEntry(testPrincipal, privs, false, null, rest));

        List<ACE> entries = acl.getEntries();
        assertEquals(1, entries.size());
        assertArrayEquals(new String[] {jcrItemNames}, entries.get(0).getRestrictionNames());
        assertArrayEquals(valArray, entries.get(0).getRestrictions(jcrItemNames));
    }
}