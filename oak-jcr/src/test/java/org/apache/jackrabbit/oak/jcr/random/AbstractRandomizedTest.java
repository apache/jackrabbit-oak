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

import java.io.File;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.junit.After;
import org.junit.Before;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;


/**
 * Base class for randomized tests.
 */
public abstract class AbstractRandomizedTest {

    private  Repository jackrabbitRepository;
    private  Repository oakRepository;

    protected  String userId = "testuser";
    protected String[] ids = new String[] {userId, "group1", "group2"};

    protected List<JackrabbitSession> writeSessions = new ArrayList();
    protected List<Session> readSessions = new ArrayList();

    @Before
    public void setUp() throws Exception {
        jackrabbitRepository = JcrUtils.getRepository(
                new File("target", "jackrabbit").toURI().toURL().toString());
        oakRepository = new Jcr().createRepository();

        writeSessions.add((JackrabbitSession) jackrabbitRepository.login(new SimpleCredentials("admin", "admin".toCharArray())));
        writeSessions.add((JackrabbitSession) oakRepository.login(new SimpleCredentials("admin", "admin".toCharArray())));

        setupAuthorizables();
        setupContent();

        readSessions.add(jackrabbitRepository.login(new SimpleCredentials(userId, userId.toCharArray())));
        readSessions.add(oakRepository.login(new SimpleCredentials(userId, userId.toCharArray())));
    }

    @After
    public void tearDown() throws Exception {
        clearContent();
        clearAuthorizables();

        for (JackrabbitSession s : writeSessions) {
            if (s.isLive()) {
                s.logout();
            }
        }

        for (Session s : readSessions) {
            if (s.isLive()) {
                s.logout();
            }
        }

        jackrabbitRepository = dispose(jackrabbitRepository);
        oakRepository = dispose(oakRepository);
    }

    protected Principal getTestPrincipal(@Nonnull JackrabbitSession session) throws RepositoryException {
        return session.getUserManager().getAuthorizable(userId).getPrincipal();
    }

    protected Principal getPrincipal(@Nonnull JackrabbitSession session, int index) throws RepositoryException {
        return session.getPrincipalManager().getPrincipal(ids[index]);

    }

    protected void setupAuthorizables() throws RepositoryException {
        for (JackrabbitSession s : writeSessions) {
            UserManager userManager = s.getUserManager();
            User user = userManager.createUser(userId, userId);

            Group group = userManager.createGroup("group1");
            group.addMember(user);

            Group group2 = userManager.createGroup("group2");
            group2.addMember(user);

            s.save();
        }
    }

    protected void clearAuthorizables() throws RepositoryException {
        for (JackrabbitSession s : writeSessions) {
            UserManager userManager = s.getUserManager();
            for (String id : ids) {
                Authorizable a = userManager.getAuthorizable(id);
                if (a != null) {
                    a.remove();
                }
            }
            s.save();
        }
    }

    protected abstract void setupContent() throws Exception;

    protected abstract void clearContent() throws Exception;
}
