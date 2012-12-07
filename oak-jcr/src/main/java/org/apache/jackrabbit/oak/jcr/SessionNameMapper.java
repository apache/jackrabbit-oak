package org.apache.jackrabbit.oak.jcr;

import javax.annotation.CheckForNull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.namepath.AbstractNameMapper;

public class SessionNameMapper extends AbstractNameMapper {
    private final SessionDelegate sessionDelegate;

    public SessionNameMapper(SessionDelegate sessionDelegate) {
        this.sessionDelegate = sessionDelegate;
    }

    @Override
    @CheckForNull
    protected String getJcrPrefix(String oakPrefix) {
        try {
            String ns = sessionDelegate.getWorkspace().getNamespaceRegistry().getURI(oakPrefix);
            return sessionDelegate.getSession().getNamespacePrefix(ns);
        } catch (RepositoryException e) {
            SessionDelegate.log.debug("Could not get JCR prefix for OAK prefix " + oakPrefix, e);
            return null;
        }
    }

    @Override
    @CheckForNull
    protected String getOakPrefix(String jcrPrefix) {
        try {
            String ns = sessionDelegate.getSession().getNamespaceURI(jcrPrefix);
            return sessionDelegate.getWorkspace().getNamespaceRegistry().getPrefix(ns);
        } catch (RepositoryException e) {
            SessionDelegate.log.debug("Could not get OAK prefix for JCR prefix " + jcrPrefix, e);
            return null;
        }
    }

    @Override
    @CheckForNull
    protected String getOakPrefixFromURI(String uri) {
        try {
            return sessionDelegate.getWorkspace().getNamespaceRegistry().getPrefix(uri);
        } catch (RepositoryException e) {
            SessionDelegate.log.debug("Could not get OAK prefix for URI " + uri, e);
            return null;
        }
    }

    @Override
    public boolean hasSessionLocalMappings() {
        if (sessionDelegate.getSession() instanceof SessionImpl) {
            return ((SessionImpl) sessionDelegate.getSession()).hasSessionLocalMappings();
        }
        else {
            // we don't know
            return true;
        }
    }
}
