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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import java.util.ArrayList;
import java.util.List;
import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionTest {

    private Repository repository;
    private List<Session> sessions = new ArrayList<Session>();

    @Before
    public void before() throws Exception {
        repository = new Jcr().createRepository();

        Session admin = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        sessions.add(admin);
        Node testNode = admin.getRootNode().addNode("testNode");
        AccessControlUtils.addAccessControlEntry(admin, testNode.getPath(),
                EveryonePrincipal.getInstance(), new String[]{Privilege.JCR_READ}, true);
        admin.save();
    }

    @After
    public void after() throws Exception {
        for (Session s : sessions) {
            s.logout();
        }
        repository = dispose(repository);
        sessions = null;
    }

    @Test
    public void testNodeIsCheckedOut() throws RepositoryException {
        Session s = repository.login(new GuestCredentials());
        sessions.add(s);

        assertFalse(s.nodeExists("/"));
        assertTrue(s.nodeExists("/testNode"));

        assertTrue(s.getWorkspace().getVersionManager().isCheckedOut("/testNode"));
    }

}