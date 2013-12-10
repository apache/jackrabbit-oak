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
package org.apache.jackrabbit.oak.jcr.random;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.junit.After;
import org.junit.Before;


/**
 * Base class for randomized tests.
 */
public abstract class AbstractRandomizedTest {

    protected  Repository jackrabbitRepository;
    protected  Repository oakRepository;

    protected  Session jackrabbitWriterSession;
    protected  Session oakWriterSession;

    protected  Session jackrabbitReaderSession;
    protected  Session oakReaderSession;

    protected  Principal jackrabbitPrincipal;
    protected  Principal oakPrincipal;

    protected  String userId = "testuser";

    protected  List<Principal> jackrabbitPrincipals = new ArrayList<Principal>();
    protected  List<Principal> oakPrincipals = new ArrayList<Principal>();

    @Before
    public void setUp() throws Exception {
        jackrabbitRepository = JcrUtils.getRepository();
        jackrabbitWriterSession = jackrabbitRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        oakRepository = new Jcr().createRepository();
        oakWriterSession = oakRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));

        jackrabbitPrincipal = setUpUser(jackrabbitWriterSession, userId);
        jackrabbitPrincipals.add(jackrabbitPrincipal);

        oakPrincipal = setUpUser(oakWriterSession, userId);
        oakPrincipals.add(oakPrincipal);

        Principal groupJR = setUpGroup(jackrabbitWriterSession, "group1", jackrabbitPrincipal);
        jackrabbitPrincipals.add(groupJR);

        Principal groupJR2 = setUpGroup(jackrabbitWriterSession, "group2", jackrabbitPrincipal);
        jackrabbitPrincipals.add(groupJR2);

        Principal groupOAK = setUpGroup(oakWriterSession, "group1", oakPrincipal);
        oakPrincipals.add(groupOAK);

        Principal groupOAK2 = setUpGroup(oakWriterSession, "group2", oakPrincipal);
        oakPrincipals.add(groupOAK2);

        oakReaderSession = oakRepository.login(new SimpleCredentials(userId, userId.toCharArray()));
        jackrabbitReaderSession = jackrabbitRepository.login(new SimpleCredentials(userId, userId.toCharArray()));

        setupTree(jackrabbitWriterSession);
        setupTree(oakWriterSession);
    }

    @After
    public void tearDown() throws Exception {

        clearTree(jackrabbitWriterSession);
        clearTree(oakWriterSession);

        removAuthorizable(jackrabbitWriterSession, userId);
        removAuthorizable(oakWriterSession, userId);

        removAuthorizable(jackrabbitWriterSession, "group1");
        removAuthorizable(oakWriterSession, "group1");
        removAuthorizable(jackrabbitWriterSession, "group2");
        removAuthorizable(oakWriterSession, "group2");

        oakPrincipals.clear();
        jackrabbitPrincipals.clear();

        if (jackrabbitWriterSession.isLive()) {
            jackrabbitWriterSession.logout();
        }

        if (oakWriterSession.isLive()) {
            oakWriterSession.logout();
        }

        if (jackrabbitReaderSession.isLive()) {
            jackrabbitReaderSession.logout();
        }

        if (oakReaderSession.isLive()) {
            oakReaderSession.logout();
        }

        jackrabbitRepository = null;
        oakRepository = null;
    }

    protected Principal setUpUser(Session session, String userId)
            throws Exception {
        UserManager userManager = ((JackrabbitSession) session).getUserManager();
        User user = userManager.createUser(userId, userId);
        session.save();
        return user.getPrincipal();
    }

    protected Principal setUpGroup(Session session, String groupId,
                                   Principal userPrincipal) throws Exception {
        UserManager userManager = ((JackrabbitSession) session).getUserManager();
        Group group = userManager.createGroup(groupId);
        group.addMember(userManager.getAuthorizable(userPrincipal));
        session.save();
        return group.getPrincipal();
    }

    protected void removAuthorizable(Session session, String id)
            throws Exception {
        UserManager userManager = ((JackrabbitSession) session).getUserManager();
        Authorizable  authorizable = userManager.getAuthorizable(id);
        if (id != null) {
            authorizable.remove();
        }
        session.save();
    }

    protected void setupPermission(Session session, Principal principal, String path,
                                   boolean allow, String... privilegeNames) throws Exception {
        AccessControlManager acm = session.getAccessControlManager();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acm, path);
        acl.addEntry(principal, AccessControlUtils.privilegesFromNames(acm, privilegeNames), allow);
        acm.setPolicy(path, acl);
        session.save();
    }

    protected abstract void setupTree(Session session) throws Exception;

    protected abstract void clearTree(Session session) throws Exception;
}
