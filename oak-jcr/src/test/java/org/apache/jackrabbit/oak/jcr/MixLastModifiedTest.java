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
        repository = new Jcr().
                with(new MixLastModifiedCommitHook()).
                createRepository();
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
        Node foo1 = root.addNode("foo1");
        Node foo2 = root.addNode("foo2");
        foo1.addMixin(NodeTypeConstants.MIX_LASTMODIFIED);
        foo2.addMixin(NodeTypeConstants.MIX_LASTMODIFIED);

        adminSession.save();
        /**
         * /foo1[jcr:mixinTypes=[mix:lastModfified]]
         * /foo2[jcr:mixinTypes=[mix:lastModfified]]
         */
        
        long foo2CreatedAt = foo2.getProperty(JcrConstants.JCR_LASTMODIFIED).getLong();
        Node bar = foo1.addNode("bar");

        adminSession.save();
        /**
         * /foo1/bar
         * /foo2
         */

        Node foobar = bar.addNode("foobar");
        foobar.addNode("node");
        long foo1TimeBeforeLastSave = System.currentTimeMillis();

        adminSession.save();
        /**
         * /foo1/bar/foobar/node
         * /foo2
         */

        long foo1TimeAfterLastSave = System.currentTimeMillis();
        session.refresh(true);
        foo1 = session.getNode("/foo1");
        foo2 = session.getNode("/foo2");
        long foo1LastModifiedAt = foo1.getProperty(JcrConstants.JCR_LASTMODIFIED).getLong();
        long foo2LastModifiedAt = foo2.getProperty(JcrConstants.JCR_LASTMODIFIED).getLong();
        assertTrue(foo1TimeBeforeLastSave < foo1LastModifiedAt && foo1LastModifiedAt < foo1TimeAfterLastSave);
        assertTrue(foo2CreatedAt == foo2LastModifiedAt);
    }
    
    @Test
    public void testLastModifiedBy() throws RepositoryException {
        adminSession.refresh(true);
        Node foo1 = adminSession.getNode("/foo1");
        Node foobar = adminSession.getNode("/foo1/bar/foobar");
        foobar.setProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, "h4x0r");
        adminSession.save();
        // TODO - once the security code allows creating nodes using other users rewrite this assertion
        assertTrue("admin".equals(foo1.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY).getString()));
    }

    private static Session createAdminSession() throws LoginException, RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }
}
