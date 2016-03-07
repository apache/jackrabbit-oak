package org.apache.jackrabbit.cluster.test.insistent;

import org.apache.jackrabbit.api.JackrabbitSession;

import javax.jcr.RepositoryException;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
public interface InsistentSession extends JackrabbitSession {

    void save(InsistentChangePack insistentChangePack) throws RepositoryException;

}
