package org.apache.jackrabbit.cluster.test.insistent.impl;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;


/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
class JackrabbitSessionWrapper extends SessionWrapper implements JackrabbitSession {

	public JackrabbitSessionWrapper(Session session) {
		super(session);
	}


	@Override
	public PrincipalManager getPrincipalManager() throws AccessDeniedException, UnsupportedRepositoryOperationException,
	RepositoryException {
		return ((JackrabbitSession) super.getSession()).getPrincipalManager();
	}

	@Override
	public UserManager getUserManager() throws AccessDeniedException, UnsupportedRepositoryOperationException, RepositoryException {
		return ((JackrabbitSession) super.getSession()).getUserManager();
	}

    /*
    @Override
	public boolean hasPermission(String s, String... strings) throws RepositoryException {
		return ((JackrabbitSession) super.getSession()).hasPermission(s, strings);
	}

	@Override
	public Item getItemOrNull(String s) throws RepositoryException {
		return ((JackrabbitSession) super.getSession()).getItemOrNull(s);
	}

	@Override
	public Property getPropertyOrNull(String s) throws RepositoryException {
		return ((JackrabbitSession) super.getSession()).getPropertyOrNull(s);
	}

	@Override
	public Node getNodeOrNull(String s) throws RepositoryException {
		return ((JackrabbitSession) super.getSession()).getNodeOrNull(s);
	}

*/

}
