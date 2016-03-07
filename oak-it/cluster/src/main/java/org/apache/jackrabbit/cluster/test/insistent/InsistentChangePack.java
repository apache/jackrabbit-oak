package org.apache.jackrabbit.cluster.test.insistent;

import javax.jcr.RepositoryException;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
public interface InsistentChangePack {

    void write() throws RepositoryException;

}
