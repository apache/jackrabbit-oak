package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.commons.JcrUtils;

import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Abstract base class for repository tests providing methods for accessing
 * the repository, a session and nodes and properties from that session.
 *
 * Uses of this class must call logout to close the session associated with
 * this instance when they are done.
 */
abstract class AbstractRepositoryTest {
    private Repository repository;
    private Session session;
    
    public void logout() {
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
            session = getRepository().login(new GuestCredentials());
        }
        return session;
    }

    protected Node getNode(String path) throws RepositoryException {
        return getSession().getNode(path);
    }

    protected Property getProperty(String path) throws RepositoryException {
        return getSession().getProperty(path);
    }

}
