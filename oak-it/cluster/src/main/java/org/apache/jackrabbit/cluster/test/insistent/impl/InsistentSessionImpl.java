package org.apache.jackrabbit.cluster.test.insistent.impl;

import org.apache.jackrabbit.cluster.test.insistent.InsistentChangePack;
import org.apache.jackrabbit.cluster.test.insistent.InsistentSession;
import org.apache.log4j.Logger;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import static java.lang.String.format;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
public class InsistentSessionImpl extends JackrabbitSessionWrapper implements InsistentSession {

    public static final Logger log = Logger.getLogger(InsistentSessionImpl.class);
    private static final int RETRIES = 10;

    public InsistentSessionImpl(Session session) {
        super(session);
    }

    @Override
    public void save() throws RepositoryException {
        throw new IllegalArgumentException("Usage of safe() not permitted. Please use save(InsistentChangePack insistentChangPack) instead");
    }

    @Override
    public void save(InsistentChangePack insistentChangePack) throws RepositoryException {
        for (int i = 1; i < RETRIES; i++) {
            try {
                saveNow(insistentChangePack);
                if (i > 1) {
                    log.warn(format("success on attempt %d", i));
                }
                return;
            } catch (RepositoryException e) {
                log.warn(format("Exception during save operation, try %d of %d, %s, (%s)", i, RETRIES, e.getMessage(), e.getCause().getCause().getMessage()));
            }
        }
        saveNow(insistentChangePack);
    }

    private void saveNow(InsistentChangePack insistentChangePack) throws RepositoryException {
        this.refresh(false);
        insistentChangePack.write();
        super.save();
    }
}
