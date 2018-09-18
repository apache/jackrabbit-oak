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

import java.security.Principal;
import java.util.Iterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * Tests for the query API exposed by {@link UserManager}.
 */
public class FindAuthorizablesRelativePathTest extends AbstractUserTest {

    private Principal p;
    private Group gr;

    private String relPath;
    private String relPath2;
    private String relPath3;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        p = getTestPrincipal();
        gr = userMgr.createGroup(p);

        Value[] vs = new Value[]{
                superuser.getValueFactory().createValue("v1"),
                superuser.getValueFactory().createValue("v2")
        };
        relPath = "relPath/" + propertyName1;
        relPath2 = "another/" + propertyName1;
        relPath3 = "relPath/child/" + propertyName1;
        gr.setProperty(relPath, vs);
        gr.setProperty(relPath2, vs);
        gr.setProperty(relPath3, superuser.getValueFactory().createValue("v3"));
        superuser.save();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        } finally{
            super.tearDown();
        }
    }

    private static void assertSingleResult(Iterator<Authorizable> result, String expectedId) throws RepositoryException {
        assertTrue("expected result", result.hasNext());
        assertEquals(expectedId, result.next().getID());
        assertFalse("expected no more results", result.hasNext());
    }

    @Test
    public void testFindV1ByName() throws NotExecutableException, RepositoryException {
        // relPath = "prop1", v = "v1" -> should find the target group
        Iterator<Authorizable> result = userMgr.findAuthorizables(propertyName1, "v1");
        assertSingleResult(result, gr.getID());
    }

    @Test
    public void testFindV3ByName() throws NotExecutableException, RepositoryException {
        // relPath = "prop1", v = "v3" -> should find the target group
        Iterator<Authorizable> result = userMgr.findAuthorizables(propertyName1, "v3");
        assertSingleResult(result, gr.getID());
    }

    @Test
    public void testFindV1ByRelativePath() throws NotExecutableException, RepositoryException {
        // relPath = "relPath/prop1", v = "v1" -> should find the target group
        Iterator<Authorizable> result = userMgr.findAuthorizables(relPath, "v1");
        assertSingleResult(result, gr.getID());
    }

    @Test
    public void testFindV1ByAltRelativePath() throws NotExecutableException, RepositoryException {
        // relPath = "another/prop1", v = "v1" -> should find the target group
        Iterator<Authorizable> result = userMgr.findAuthorizables(relPath2, "v1");
        assertSingleResult(result, gr.getID());
    }

    @Test
    public void testFindV1AtGroupNode() throws NotExecutableException, RepositoryException {
        // relPath : "./prop1", v = "v1" -> should not find the target group
        Iterator<Authorizable> result = userMgr.findAuthorizables("./" + propertyName1, "v1");
        assertFalse("expected result", result.hasNext());
    }

    @Test
    public void testFindV3AtGroupNode() throws NotExecutableException, RepositoryException {
        // relPath : "./prop1", v = "v3" -> should not find the target group
        Iterator<Authorizable> result = userMgr.findAuthorizables("./" + propertyName1, "v3");
        assertFalse("expected result", result.hasNext());
    }
}