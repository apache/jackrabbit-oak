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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

/**
 * Tests to assert that all user mgt methods that include name/path conversion
 * from JCR to OAK and back are properly implemented.
 */
public class RemappingTest extends AbstractUserTest {

    private Session session;
    private Authorizable authorizable;

    private List<String> unmappedPaths = ImmutableList.of("uTest:property", "uTest:node/uTest:property2");
    private List<String> mappedPaths = ImmutableList.of("my:property", "my:node/my:property2");
    private Value nameValue;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        superuser.getWorkspace().getNamespaceRegistry().registerNamespace("uTest", "http://jackrabbit-oak.apache.org");
        Value value = superuser.getValueFactory().createValue("value");
        nameValue = superuser.getValueFactory().createValue("uTest:value", PropertyType.NAME);
        for (String relPath : unmappedPaths) {
            user.setProperty(relPath, value);
        }
        superuser.save();

        session = getHelper().getSuperuserSession();
        session.setNamespacePrefix("my", "http://jackrabbit-oak.apache.org");
        session.setNamespacePrefix("myRep", NamespaceConstants.NAMESPACE_REP);

        authorizable = getUserManager(session).getAuthorizable(user.getID());
    }

    @Override
    protected void tearDown() throws Exception {
        session.logout();
        super.tearDown();
    }

    @Test
    public void testGetAuthorizablePath() throws Exception {
        assertTrue(user.getPath().startsWith(UserConstants.DEFAULT_USER_PATH));

        String path = authorizable.getPath();
        assertFalse(path.startsWith(UserConstants.DEFAULT_USER_PATH));
    }

    @Test
    public void testGetAuthorizableByPath() throws Exception {
        assertNotNull(getUserManager(session).getAuthorizableByPath(authorizable.getPath()));
    }

    @Test
    public void testGetAuthorizableByPath2() throws Exception {
        try {
            getUserManager(session).getAuthorizableByPath(user.getPath());
            fail("invalid path must be detected");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testFindAuthorizable() throws Exception {
        user.setProperty("prop", superuser.getValueFactory().createValue(true));
        superuser.save();
        session.refresh(false);

        Map<String, String> m = ImmutableMap.of("prop", "true", "my:property", "value", "my:node/my:property2", "value");
        for (String relPath : m.keySet()) {
            String value = m.get(relPath);
            Iterator<Authorizable> result = getUserManager(session).findAuthorizables(relPath, value);
            assertTrue(result.hasNext());
            assertEquals(user.getID(), result.next().getID());
        }
    }

    @Test
    public void testFindAuthorizable2() throws Exception {
        for (String relPath : unmappedPaths) {
            user.setProperty(relPath, nameValue);
        }
        superuser.save();
        session.refresh(false);

        Map<String, String> m = ImmutableMap.of("my:property", "my:value", "my:node/my:property2", "my:value");
        for (String relPath : m.keySet()) {
            String value = m.get(relPath);
            Iterator<Authorizable> result = getUserManager(session).findAuthorizables(relPath, value);
            assertTrue(result.hasNext());
            assertEquals(user.getID(), result.next().getID());
        }
    }

    @Test
    public void testFindAuthorizable3() throws Exception {
        for (String relPath : unmappedPaths) {
            user.setProperty(relPath, nameValue);
        }
        superuser.save();
        session.refresh(false);

        Map<String, String> m = ImmutableMap.of("my:property", "my:value", "my:node/my:property2", "my:value");
        for (String relPath : m.keySet()) {
            String value = m.get(relPath);
            Iterator<Authorizable> result = getUserManager(session).findAuthorizables(relPath, value, UserManager.SEARCH_TYPE_USER);
            assertTrue(result.hasNext());
            assertEquals(user.getID(), result.next().getID());
        }
    }

    @Test
    public void testQuery() throws Exception {
        Iterator<Authorizable> result = getUserManager(session).findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                            eq("@my:property", vf.createValue("value")));
            }
        });

        assertTrue(result.hasNext());
        assertEquals(user.getID(), result.next().getID());
    }

    @Test
    public void testGetPropertyNames() throws Exception {
        Iterator it = authorizable.getPropertyNames();
        assertTrue(it.hasNext());
        assertEquals("my:property", it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetPropertyNames2() throws Exception {
        Iterator it = authorizable.getPropertyNames("my:node");
        assertTrue(it.hasNext());
        assertEquals("my:property2", it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetPropertyNames3() throws Exception {
        try {
            Iterator it = authorizable.getPropertyNames("uTest:node");
            fail();
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testHasProperty() throws Exception {
        for (String relPath : mappedPaths) {
            assertTrue(authorizable.hasProperty(relPath));
        }
    }

    @Test
    public void testHasProperty2() throws Exception {
        for (String relPath : unmappedPaths) {
            try {
                authorizable.hasProperty(relPath);
                fail();
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testGetProperty() throws Exception {
        for (String relPath : unmappedPaths) {
            user.setProperty(relPath, nameValue);
            user.setProperty(relPath, new Value[] {nameValue});
        }
        superuser.save();
        session.refresh(false);

        for (String relPath : mappedPaths) {
            Value[] values = authorizable.getProperty(relPath);
            assertNotNull(values);
            assertEquals(1, values.length);
            if (PropertyType.STRING == values[0].getType()) {
                assertEquals("value", values[0].getString());
            } else {
                assertEquals("my:value", values[0].getString());
            }
        }
    }

    @Test
    public void testGetProperty2() throws Exception {
        for (String relPath : unmappedPaths) {
            try {
                authorizable.getProperty(relPath);
                fail();
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testSetProperty() throws Exception {
        for (String relPath : mappedPaths) {
            authorizable.setProperty(relPath, nameValue);
            authorizable.setProperty(relPath, new Value[] {nameValue});
        }
    }

    @Test
    public void testSetProperty2() throws Exception {
        for (String relPath : unmappedPaths) {
            try {
                authorizable.setProperty(relPath, nameValue);
                fail();
            } catch (RepositoryException e) {
                // success
            }

            try {
                authorizable.setProperty(relPath, new Value[] {nameValue});
                fail();
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testRemoveProperty() throws Exception {
        for (String relPath : mappedPaths) {
            authorizable.removeProperty(relPath);
        }
    }

    @Test
    public void testRemoveProperty2() throws Exception {
        for (String relPath : unmappedPaths) {
            try {
                authorizable.removeProperty(relPath);
                fail();
            } catch (RepositoryException e) {
                // success
            }
        }
    }
}