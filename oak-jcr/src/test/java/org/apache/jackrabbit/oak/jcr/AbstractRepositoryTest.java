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

import org.apache.jackrabbit.commons.JcrUtils;

import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Abstract base class for repository tests providing methods for accessing
 * the repository, a session and nodes and properties from that session.
 *
 * Users of this class must call clear to close the session associated with
 * this instance and clean up the repository when done.
 */
public abstract class AbstractRepositoryTest {
    private Repository repository;
    private Session session;

    public void logout() throws RepositoryException {
        Session cleanupSession = createAnonymousSession();
        try {
            Node root = cleanupSession.getRootNode();
            NodeIterator ns = root.getNodes();
            while (ns.hasNext()) {
                ns.nextNode().remove();
            }
            cleanupSession.save();
        }
        finally {
            cleanupSession.logout();
        }

        // release session field
        if (session != null) {
            session.logout();
            session = null;
        }
    }

    protected Repository getRepository() throws RepositoryException {
        if (repository == null) {
            repository = JcrUtils.getRepository("jcr-oak://inmemory/CRUDTest");
        }

        return repository;
    }

    protected Session getSession() throws RepositoryException {
        if (session == null) {
            session = createAnonymousSession();
        }
        return session;
    }

    protected Node getNode(String path) throws RepositoryException {
        return getSession().getNode(path);
    }

    protected Property getProperty(String path) throws RepositoryException {
        return getSession().getProperty(path);
    }

    protected Session createAnonymousSession() throws RepositoryException {
        return getRepository().login(new GuestCredentials());
    }

}
