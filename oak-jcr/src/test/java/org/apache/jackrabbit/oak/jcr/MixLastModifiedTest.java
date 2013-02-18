/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertTrue;

import java.util.Calendar;

import javax.jcr.LoginException;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.plugins.commit.MixLastModifiedCommitHook;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MixLastModifiedTest {

    public static final String TEST_USER_NAME = "user1";
    public static final String TEST_USER_PASSWORD = "user1";

    private static Repository repository;

    private static Session adminSession;
    private static Session session;
    private static User user;

    @BeforeClass
    public static void beforeClass() throws LoginException, RepositoryException {
        repository = new Jcr().with(new MixLastModifiedCommitHook()).createRepository();
        adminSession = createAdminSession();
        UserManager usrMgr = ((JackrabbitSession) adminSession).getUserManager();
        user = usrMgr.createUser(TEST_USER_NAME, TEST_USER_PASSWORD);
        adminSession.save();
        session = repository.login(new SimpleCredentials(user.getID(), TEST_USER_PASSWORD.toCharArray()));
    }

    @AfterClass
    public static void tearDown() {
        if (adminSession != null) {
            adminSession.logout();
        }
        if (session != null) {
            session.logout();
        }
    }

    @Test
    public void testLastModifiedDate() throws LoginException, RepositoryException {
        Node root = adminSession.getRootNode();
        Node foo = root.addNode("foo");
        adminSession.save();
        Node foo1 = foo.addNode("foo1");
        Node foo2 = root.addNode("foo2");
        foo1.addMixin(NodeTypeConstants.MIX_LASTMODIFIED);
        Node bar = foo1.addNode("bar");
        foo2.addMixin(NodeTypeConstants.MIX_LASTMODIFIED);

        adminSession.save();
        /**
         *      /foo/foo1[jcr:mixinTypes=[mix:lastModfified]]/bar
         *      /foo2[jcr:mixinTypes=[mix:lastModfified]]
         */

        long foo2CreatedAt = foo2.getProperty(JcrConstants.JCR_LASTMODIFIED).getLong();

        Node foobar = bar.addNode("foobar");
        foobar.addNode("node");
        long foo1TimeBeforeSave = System.currentTimeMillis();

        adminSession.save();
        /**
         *      /foo/foo1/bar/foobar/node
         *      /foo2
         */

        long foo1TimeAfterSave = System.currentTimeMillis();
        session.refresh(true);
        foo1 = session.getNode("/foo/foo1");
        foo2 = session.getNode("/foo2");
        long foo1LastModifiedAt = foo1.getProperty(JcrConstants.JCR_LASTMODIFIED).getLong();
        long foo2LastModifiedAt = foo2.getProperty(JcrConstants.JCR_LASTMODIFIED).getLong();
        assertTrue(foo1TimeBeforeSave < foo1LastModifiedAt && foo1LastModifiedAt < foo1TimeAfterSave);
        assertTrue(foo2CreatedAt == foo2LastModifiedAt);
        
        foo1TimeBeforeSave = System.currentTimeMillis();
        foobar.remove();
        adminSession.save();
        session.refresh(true);
        foo1 = session.getNode("/foo/foo1");
        foo1LastModifiedAt = foo1.getProperty(JcrConstants.JCR_LASTMODIFIED).getLong();
        foo1TimeAfterSave = System.currentTimeMillis();
        assertTrue(foo1TimeBeforeSave < foo1LastModifiedAt && foo1LastModifiedAt < foo1TimeAfterSave);

        Calendar calendar = Calendar.getInstance();
        long explicitLastModifiedDate = calendar.getTimeInMillis();
        foo1 = adminSession.getNode("/foo/foo1");
        foo1.setProperty(JcrConstants.JCR_LASTMODIFIED, calendar);
        adminSession.save();
        session.refresh(true);
        foo1 = session.getNode("/foo/foo1");
        foo1LastModifiedAt = foo1.getProperty(JcrConstants.JCR_LASTMODIFIED).getLong();
        assertTrue(explicitLastModifiedDate == foo1LastModifiedAt);
    }

    @Test
    public void testLastModifiedBy() throws RepositoryException {
        adminSession.refresh(true);
        Node foo1 = adminSession.getNode("/foo/foo1");
        Node foo2 = adminSession.getNode("/foo2");
        foo1.setProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, TEST_USER_NAME);
        adminSession.save();
        
        session.refresh(true);
        foo1 = session.getNode("/foo/foo1");
        foo2 = session.getNode("/foo2");
        assertTrue(TEST_USER_NAME.equals(foo1.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY).getString()));
        assertTrue("admin".equals(foo2.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY).getString()));
        
        adminSession.refresh(true);
        foo1 = adminSession.getNode("/foo/foo1");
        foo1.setProperty("foo1", "foo1");
        adminSession.save();
        
        session.refresh(true);
        foo1 = session.getNode("/foo/foo1");
        assertTrue("admin".equals(foo1.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY).getString()));
    }

    private static Session createAdminSession() throws LoginException, RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }
}
