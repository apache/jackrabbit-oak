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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * Base class for user import related tests.
 */
public abstract class AbstractImportTest {

    private static final String ADMINISTRATORS = "administrators";
    protected static final String USERPATH = "/rep:security/rep:authorizables/rep:users";
    protected static final String GROUPPATH = "/rep:security/rep:authorizables/rep:groups";

    private Repository repo;
    protected Session adminSession;
    protected UserManager userMgr;

    @Before
    public void before() throws Exception {

        String importBehavior = getImportBehavior();
        SecurityProvider securityProvider;
        if (importBehavior != null) {
            Map<String, String> userParams = new HashMap();
            userParams.put(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior());
            ConfigurationParameters config = ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME, ConfigurationParameters.of(userParams)));

            securityProvider = new SecurityProviderImpl(config);
        } else {
            securityProvider = new SecurityProviderImpl();
        }
        Jcr jcr = new Jcr();
        jcr.with(securityProvider);
        repo = jcr.createRepository();
        adminSession = repo.login(new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID.toCharArray()));

        if (!(adminSession instanceof JackrabbitSession)) {
            throw new NotExecutableException();
        }
        userMgr = ((JackrabbitSession) adminSession).getUserManager();

        // make sure the target node for group-import exists
        Authorizable administrators = userMgr.getAuthorizable(ADMINISTRATORS);
        if (administrators == null) {
            administrators = userMgr.createGroup(new PrincipalImpl(ADMINISTRATORS));
            adminSession.save();
        } else if (!administrators.isGroup()) {
            throw new NotExecutableException("Expected " + administrators.getID() + " to be a group.");
        }
        adminSession.save();
    }

    @After
    public void after() throws Exception {
        try {
            adminSession.refresh(false);
            NodeIterator intermediateNodes = adminSession.getNode(GROUPPATH).getNodes();
            while (intermediateNodes.hasNext()) {
                intermediateNodes.nextNode().remove();
            }
            String builtinPath = USERPATH + "/a";
            intermediateNodes = adminSession.getNode(USERPATH).getNodes();
            while (intermediateNodes.hasNext()) {
                Node n = intermediateNodes.nextNode();
                if (!builtinPath.equals(n.getPath())) {
                    n.remove();
                }
            }
            adminSession.save();
        } finally {
            if (getImportBehavior() != null) {
                adminSession.logout();
                repo = null;
            }
        }
    }

    protected abstract String getImportBehavior();

    protected abstract String getTargetPath();

    protected Node getTargetNode() throws RepositoryException {
        return adminSession.getNode(getTargetPath());
    }

    protected String getExistingUUID() throws RepositoryException {
        Node n = adminSession.getRootNode();
        n.addMixin(JcrConstants.MIX_REFERENCEABLE);
        return n.getUUID();
    }

    protected void doImport(String parentPath, String xml) throws Exception {
        doImport(parentPath, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
    }

    protected void doImport(String parentPath, String xml, int importUUIDBehavior) throws Exception {
        InputStream in;
        if (xml.charAt(0) == '<') {
            in = new ByteArrayInputStream(xml.getBytes());
            // uncomment to dump include XMLs
            // FileOutputStream out = new FileOutputStream(getTestXml());
            // out.write(xml.getBytes());
            // out.close();
        } else {
            in = getClass().getResourceAsStream(xml);
        }
        try {
            adminSession.importXML(parentPath, in, importUUIDBehavior);
        } finally {
            in.close();
        }
    }

    protected static void assertNotDeclaredMember(Group gr, String potentialID, Session session) throws RepositoryException {
        // declared members must not list the invalid entry.
        Iterator<Authorizable> it = gr.getDeclaredMembers();
        while (it.hasNext()) {
            Authorizable member = it.next();
            assertFalse(potentialID.equals(session.getNode(member.getPath()).getIdentifier()));
        }
    }

    private String getTestXml() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTraceElements) {
            try {
                Class<?> clazz = Class.forName(element.getClassName());
                for (Method method : clazz.getMethods()){
                    if(method.getName().equals(element.getMethodName())){
                        if (method.getAnnotation(Test.class) != null) {
                            return clazz.getSimpleName() + "-" + method.getName() + ".xml";
                        }
                    }

                }
            } catch (Exception e) {
                //  oops do something here
            }
        }
        throw new IllegalArgumentException("no import xml given.");
    }
}
