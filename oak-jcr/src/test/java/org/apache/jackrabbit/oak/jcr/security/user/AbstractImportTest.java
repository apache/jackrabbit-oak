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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for user import related tests.
 */
public abstract class AbstractImportTest extends AbstractJCRTest {

    private static final String ADMINISTRATORS = "administrators";
    protected static final String USERPATH = "/rep:security/rep:authorizables/rep:users";
    protected static final String GROUPPATH = "/rep:security/rep:authorizables/rep:groups";


    private boolean removeAdministrators;

    private Repository repo;
    protected Session adminSession;
    protected UserManager userMgr;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        String importBehavior = getImportBehavior();
        if (importBehavior != null) {
            Map<String,String> userParams = new HashMap();
            userParams.put(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior());
            ConfigurationParameters config = new ConfigurationParameters(ImmutableMap.of(UserConfiguration.PARAM_USER_OPTIONS, new ConfigurationParameters(userParams)));

            SecurityProvider securityProvider = new SecurityProviderImpl(config);
            String dir = "target/mk-tck-" + System.currentTimeMillis();
            Jcr jcr = new Jcr(new MicroKernelImpl(dir));
            jcr.with(Executors.newScheduledThreadPool(1))
                    .with(securityProvider);
            repo = jcr.createRepository();
            adminSession = repo.login(getHelper().getSuperuserCredentials());
        } else {
            adminSession = superuser;
        }

        if (!(adminSession instanceof JackrabbitSession)) {
            throw new NotExecutableException();
        }
        userMgr = ((JackrabbitSession) adminSession).getUserManager();

        // avoid collision with testing a-folders that may have been created
        // with another test (but not removed as user/groups got removed)
        String path = USERPATH + "/t";
        if (adminSession.nodeExists(path)) {
            adminSession.getNode(path).remove();
        }
        path = GROUPPATH + "/g";
        if (adminSession.nodeExists(path)) {
            adminSession.getNode(path).remove();
        }

        // make sure the target node for group-import exists
        Authorizable administrators = userMgr.getAuthorizable(ADMINISTRATORS);
        if (administrators == null) {
            userMgr.createGroup(new PrincipalImpl(ADMINISTRATORS));
            adminSession.save();
            removeAdministrators = true;
        } else if (!administrators.isGroup()) {
            throw new NotExecutableException("Expected " + administrators.getID() + " to be a group.");
        }
        adminSession.save();
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        try {
            adminSession.refresh(false);

            String path = USERPATH + "/t";
            if (adminSession.nodeExists(path)) {
                adminSession.getNode(path).remove();
            }
            path = GROUPPATH + "/g";
            if (adminSession.nodeExists(path)) {
                adminSession.getNode(path).remove();
            }
            if (removeAdministrators) {
                Authorizable a = userMgr.getAuthorizable(ADMINISTRATORS);
                if (a != null) {
                    a.remove();
                }
            }
            adminSession.save();
        } finally {
            if (getImportBehavior() != null) {
                adminSession.logout();
                repo = null;
            }
            super.tearDown();
        }
    }

    protected abstract String getImportBehavior();

    protected String getExistingUUID() throws RepositoryException {
        Node n = adminSession.getRootNode();
        n.addMixin(mixReferenceable);
        return n.getUUID();
    }

    protected void doImport(String parentPath, String xml) throws Exception {
        InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
        adminSession.importXML(parentPath, in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
    }

    protected void doImport(String parentPath, String xml, int importUUIDBehavior) throws Exception {
        InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
        adminSession.importXML(parentPath, in, importUUIDBehavior);
    }

    protected static void assertNotDeclaredMember(Group gr, String potentialID, Session session ) throws RepositoryException {
        // declared members must not list the invalid entry.
        Iterator<Authorizable> it = gr.getDeclaredMembers();
        while (it.hasNext()) {
            Authorizable member = it.next();
            assertFalse(potentialID.equals(session.getNode(member.getPath()).getIdentifier()));
        }
    }
}