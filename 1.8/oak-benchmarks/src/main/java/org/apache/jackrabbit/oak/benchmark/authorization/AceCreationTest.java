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
package org.apache.jackrabbit.oak.benchmark.authorization;

import java.util.Random;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.benchmark.AbstractTest;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

public class AceCreationTest extends AbstractTest {

    public static final int NUMBER_OF_INITIAL_ACE_DEFAULT = 0;
    private final int numberOfAce;
    private final int numberOfInitialAce;
    private final boolean transientWrites;
    private String nodePath;

    private Session transientSession;

    public AceCreationTest(int numberOfAce, int numberOfInitialAce, boolean transientWrites) {
        super();
        this.numberOfAce = numberOfAce;
        this.numberOfInitialAce = numberOfInitialAce;
        this.transientWrites = transientWrites;
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        Session session = createOrGetSystemSession();
        nodePath = session.getRootNode().addNode("test" + new Random().nextInt()).getPath();

        save(session, transientWrites);
        logout(session, transientWrites);
    }

    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();

        Session session = createOrGetSystemSession();
        createAce(session, numberOfInitialAce);

        save(session, transientWrites);
        logout(session, transientWrites);
    }

    @Override
    protected void afterTest() throws Exception {
        Session session = createOrGetSystemSession();

        AccessControlManager acm = session.getAccessControlManager();
        for (AccessControlPolicy policy : acm.getPolicies(nodePath)) {
            acm.removePolicy(nodePath, policy);
        }
        save(session, transientWrites);

        super.afterTest();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            if (transientSession != null) {
                transientSession.logout();
            }
        } finally {
            super.tearDown();
        }
    }

    @Override
    protected void runTest() throws Exception {
        Session session = createOrGetSystemSession();

        createAce(session, numberOfAce);
        save(session, transientWrites);
        logout(session, transientWrites);
    }

    private void createAce(Session session, int count) throws RepositoryException {
        AccessControlManager acManager = session.getAccessControlManager();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acManager, nodePath);

        for (int i = 0; i < count; i++) {
            ImmutableMap<String, Value> restrictions = ImmutableMap.of(AccessControlConstants.REP_GLOB, session.getValueFactory().createValue(i + ""));
            acl.addEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(acManager, Privilege.JCR_ADD_CHILD_NODES), true, restrictions);
        }

        acManager.setPolicy(nodePath, acl);
    }

    private static void save(Session session, boolean transientWrites) throws RepositoryException {
        if (!transientWrites) {
            session.save();
        }
    }

    private static void logout(Session session, boolean transientWrites) {
        if (!transientWrites) {
            session.logout();
        }
    }

    private Session createOrGetSystemSession() {
        if(transientWrites && transientSession != null) {
            return transientSession;
        }

        return (transientSession = systemLogin());
    }
}
