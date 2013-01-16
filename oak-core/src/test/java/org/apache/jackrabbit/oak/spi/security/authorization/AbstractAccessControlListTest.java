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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * AbstractAccessControlListTest... TODO
 */
public abstract class AbstractAccessControlListTest extends AbstractSecurityTest {

    private final String testPath = "/testPath";
    private PrivilegeManager privMgr;
    private RestrictionProvider restrictionProvider;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        privMgr = getSecurityProvider().getPrivilegeConfiguration().getPrivilegeManager(root, namePathMapper);
        restrictionProvider = getSecurityProvider().getAccessControlConfiguration().getRestrictionProvider(namePathMapper);
    }

    protected String getTestPath() {
        return testPath;
    }

    protected PrivilegeManager getPrivilegeManager() {
        return privMgr;
    }

    protected RestrictionProvider getRestrictionProvider() {
        return restrictionProvider;
    }

    protected AbstractAccessControlList createACL(List<JackrabbitAccessControlEntry> entries) {
        return createACL(getTestPath(), entries);
    }

    protected AbstractAccessControlList createACL(String jcrPath, List<JackrabbitAccessControlEntry> entries) {
        return createACL(jcrPath, entries, namePathMapper);
    }

    protected abstract AbstractAccessControlList createACL(String jcrPath,
                                                           List<JackrabbitAccessControlEntry> entries,
                                                           NamePathMapper namePathMapper);

    protected List<JackrabbitAccessControlEntry> createTestEntries(int size) throws RepositoryException {
        if (size == 0) {
            return Collections.emptyList();
        } else {
            List<JackrabbitAccessControlEntry> entries = new ArrayList<JackrabbitAccessControlEntry>(size);
            for (int i = 0; i < size; i++) {
                entries.add(new ACE(
                        new PrincipalImpl("testPrincipal"+i),
                        new Privilege[] {getPrivilegeManager().getPrivilege(PrivilegeConstants.JCR_READ)},
                        true, null));
            }
            return entries;
        }
    }

    @Test
    public void testGetPath() {
        NameMapper nameMapper = new GlobalNameMapper() {
            @Override
            protected Map<String, String> getNamespaceMap() {
                return Collections.singletonMap("jr", "http://jackrabbit.apache.org");
            }
        };
        NamePathMapper npMapper = new NamePathMapperImpl(nameMapper);

        // map of jcr-path to standard jcr-path
        Map<String, String> paths = new HashMap<String, String>();
        paths.put(null, null);
        paths.put(getTestPath(), getTestPath());
        paths.put("/", "/");
        paths.put("/jr:testPath", "/jr:testPath");
        paths.put("/{http://jackrabbit.apache.org}testPath", "/jr:testPath");

        for (String path : paths.keySet()) {
            AbstractAccessControlList acl = createACL(path, Collections.<JackrabbitAccessControlEntry>emptyList(), npMapper);
            assertEquals(paths.get(path), acl.getPath());
        }
    }

    @Test
    public void testGetOakPath() {
        NameMapper nameMapper = new LocalNameMapper(Collections.singletonMap("my", "http://jackrabbit.apache.org")) {
            @Override
            protected Map<String, String> getNamespaceMap() {
                return Collections.singletonMap("jr", "http://jackrabbit.apache.org");
            }
        };
        NamePathMapper npMapper = new NamePathMapperImpl(nameMapper);

        // map of jcr-path to oak path
        Map<String, String> paths = new HashMap<String, String>();
        paths.put(null, null);
        paths.put(getTestPath(), getTestPath());
        paths.put("/", "/");
        paths.put("/my:testPath", "/jr:testPath");
        paths.put("/{http://jackrabbit.apache.org}testPath", "/jr:testPath");

        // test if oak-path is properly set.
        for (String path : paths.keySet()) {
            AbstractAccessControlList acl = createACL(path, Collections.<JackrabbitAccessControlEntry>emptyList(), npMapper);
            assertEquals(paths.get(path), acl.getOakPath());
        }
    }

    @Test
    public void testGetEntries() {
        AbstractAccessControlList acl = createACL(Collections.<JackrabbitAccessControlEntry>emptyList());


    }

    @Test
    public void testGetAccessControlEntries() {

    }

    @Test
    public void testSize() throws RepositoryException {
        AbstractAccessControlList acl = createACL(createTestEntries(0));
        assertEquals(0, acl.size());

        acl = createACL(createTestEntries(3));
        assertEquals(3, acl.size());
    }

    @Test
    public void testIsEmpty() throws RepositoryException {
        AbstractAccessControlList acl = createACL(createTestEntries(0));
        assertTrue(acl.isEmpty());

        acl = createACL(createTestEntries(3));
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testGetRestrictionNames() throws RepositoryException {
        AbstractAccessControlList acl = createACL(createTestEntries(0));
        List<String> names = Lists.newArrayList(acl.getRestrictionNames());
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(getTestPath())) {
            assertTrue(names.remove(def.getJcrName()));
        }
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetRestrictionType() throws RepositoryException {
        AbstractAccessControlList acl = createACL(createTestEntries(0));
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(getTestPath())) {
            int reqType = acl.getRestrictionType(def.getJcrName());
            assertEquals(def.getRequiredType(), reqType);
        }
    }
}