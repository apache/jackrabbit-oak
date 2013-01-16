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
package org.apache.jackrabbit.oak.security.user.query;

import java.util.Iterator;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * UserQueryManagerTest provides test cases for {@link UserQueryManager}.
 * This class include the original jr2.x test-cases provided by
 * {@code NodeResolverTest} and {@code IndexNodeResolverTest}.
 */
public class UserQueryManagerTest extends AbstractSecurityTest {

    private ValueFactory valueFactory;
    private UserQueryManager queryMgr;
    private User user;
    private String propertyName;

    @Before
    public void before() throws Exception {
        super.before();

        UserManager userMgr = getUserManager();
        user = userMgr.createUser("testUser", "pw");
        root.commit();

        queryMgr = new UserQueryManager(userMgr, namePathMapper, getUserConfiguration().getConfigurationParameters(), root.getQueryEngine());

        valueFactory = new ValueFactoryImpl(root.getBlobFactory(), namePathMapper);
        propertyName = "testProperty";
    }

    /**
     * @since oak
     */
    @Test
    public void testFindNodesExact() throws Exception {
        Value vs = valueFactory.createValue("value \\, containing backslash");
        user.setProperty(propertyName, vs);
        root.commit();

        try {
            Iterator<Authorizable> result = queryMgr.findAuthorizables(propertyName, "value \\, containing backslash", AuthorizableType.USER, true);
            assertTrue("expected result", result.hasNext());
            assertEquals(user.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());
        } finally {
            user.removeProperty(propertyName);
            root.commit();
        }
    }

    @Test
    public void testFindNodesNonExact() throws Exception {
        Value vs = valueFactory.createValue("value \\, containing backslash");
        user.setProperty(propertyName, vs);
        root.commit();

        try {
            Iterator<Authorizable> result = queryMgr.findAuthorizables(propertyName, "value \\, containing backslash", AuthorizableType.USER, false);
            assertTrue("expected result", result.hasNext());
            assertEquals(user.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());
        } finally {
            user.removeProperty(propertyName);
            root.commit();
        }
    }

    @Test
    public void testFindNodesNonExactWithApostrophe() throws Exception {
        Value vs = valueFactory.createValue("value ' with apostrophe");
        try {
            user.setProperty(propertyName, vs);
            root.commit();

            Iterator<Authorizable> result = queryMgr.findAuthorizables(propertyName, "value ' with apostrophe", AuthorizableType.USER, false);

            assertTrue("expected result", result.hasNext());
            assertEquals(user.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());
        } finally {
            user.removeProperty(propertyName);
            root.commit();
        }
    }

    @Test
    public void testFindNodesExactWithApostrophe() throws Exception {
        Value vs = valueFactory.createValue("value ' with apostrophe");
        try {
            user.setProperty(propertyName, vs);
            root.commit();

            Iterator<Authorizable> result = queryMgr.findAuthorizables(propertyName, "value ' with apostrophe", AuthorizableType.USER, true);
            assertTrue("expected result", result.hasNext());
            assertEquals(user.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());
        } finally {
            user.removeProperty(propertyName);
            root.commit();
        }
    }
}