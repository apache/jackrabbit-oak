package org.apache.jackrabbit.cluster.test.insistent.impl;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import javax.jcr.*;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.retention.RetentionManager;
import javax.jcr.security.AccessControlManager;
import javax.jcr.version.VersionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AccessControlException;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
class SessionWrapper implements Session {

	private final Session session;

	public SessionWrapper(Session session) {
		if (session == null) {
			throw new IllegalArgumentException("session == null");
		}
		this.session = session;
	}

	public Session getSession() {
		return session;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void addLockToken(String lt) {
		session.addLockToken(lt);
	}

	@Override
	public void checkPermission(String absPath, String actions)
			throws AccessControlException, RepositoryException {
		session.checkPermission(absPath, actions);
	}

	@Override
	public void exportDocumentView(String absPath,
								   ContentHandler contentHandler, boolean skipBinary, boolean noRecurse)
			throws PathNotFoundException, SAXException, RepositoryException {
		session.exportDocumentView(absPath, contentHandler, skipBinary,
				noRecurse);
	}

	@Override
	public void exportDocumentView(String absPath, OutputStream out,
								   boolean skipBinary, boolean noRecurse) throws IOException,
			PathNotFoundException, RepositoryException {
		session.exportDocumentView(absPath, out, skipBinary, noRecurse);
	}

	@Override
	public void exportSystemView(String absPath, ContentHandler contentHandler,
								 boolean skipBinary, boolean noRecurse)
			throws PathNotFoundException, SAXException, RepositoryException {
		session.exportSystemView(absPath, contentHandler, skipBinary, noRecurse);
	}

	@Override
	public void exportSystemView(String absPath, OutputStream out,
								 boolean skipBinary, boolean noRecurse) throws IOException,
			PathNotFoundException, RepositoryException {
		session.exportSystemView(absPath, out, skipBinary, noRecurse);
	}

	@Override
	public AccessControlManager getAccessControlManager()
			throws UnsupportedRepositoryOperationException, RepositoryException {
		return session.getAccessControlManager();
	}

	@Override
	public Object getAttribute(String name) {
		return session.getAttribute(name);
	}

	@Override
	public String[] getAttributeNames() {
		return session.getAttributeNames();
	}

	@Override
	public ContentHandler getImportContentHandler(String parentAbsPath,
												  int uuidBehavior) throws PathNotFoundException,
			ConstraintViolationException, VersionException, LockException,
			RepositoryException {
		return session.getImportContentHandler(parentAbsPath, uuidBehavior);
	}

	@Override
	public Item getItem(String absPath) throws PathNotFoundException,
			RepositoryException {
		return session.getItem(absPath);
	}

	@SuppressWarnings("deprecation")
	@Override
	public String[] getLockTokens() {
		return session.getLockTokens();
	}

	@Override
	public String getNamespacePrefix(String uri) throws NamespaceException,
			RepositoryException {
		return session.getNamespacePrefix(uri);
	}

	@Override
	public String[] getNamespacePrefixes() throws RepositoryException {
		return session.getNamespacePrefixes();
	}

	@Override
	public String getNamespaceURI(String prefix) throws NamespaceException,
			RepositoryException {
		return session.getNamespaceURI(prefix);
	}

	@Override
	public Node getNode(String absPath) throws PathNotFoundException,
			RepositoryException {
		return session.getNode(absPath);
	}

	@Override
	public Node getNodeByIdentifier(String id) throws ItemNotFoundException,
			RepositoryException {
		return session.getNodeByIdentifier(id);
	}

	@SuppressWarnings("deprecation")
	@Override
	public Node getNodeByUUID(String uuid) throws ItemNotFoundException,
			RepositoryException {
		return session.getNodeByUUID(uuid);
	}

	@Override
	public Property getProperty(String absPath) throws PathNotFoundException,
			RepositoryException {
		return session.getProperty(absPath);
	}

	@Override
	public Repository getRepository() {
		return session.getRepository();
	}

	@Override
	public RetentionManager getRetentionManager()
			throws UnsupportedRepositoryOperationException, RepositoryException {
		return session.getRetentionManager();
	}

	@Override
	public Node getRootNode() throws RepositoryException {
		return session.getRootNode();
	}

	@Override
	public Session impersonate(Credentials credentials) throws LoginException, RepositoryException {
		return session.impersonate(credentials);
	}

	@Override
	public String getUserID() {
		return session.getUserID();
	}

	@Override
	public ValueFactory getValueFactory()
			throws UnsupportedRepositoryOperationException, RepositoryException {
		return session.getValueFactory();
	}

	@Override
	public Workspace getWorkspace() {
		return session.getWorkspace();
	}

	@Override
	public boolean hasCapability(String methodName, Object target,
								 Object[] arguments) throws RepositoryException {
		return session.hasCapability(methodName, target, arguments);
	}

	@Override
	public boolean hasPendingChanges() throws RepositoryException {
		return session.hasPendingChanges();
	}

	@Override
	public boolean hasPermission(String absPath, String actions)
			throws RepositoryException {
		return session.hasPermission(absPath, actions);
	}

	@Override
	public void importXML(String parentAbsPath, InputStream in, int uuidBehavior)
			throws IOException, PathNotFoundException, ItemExistsException,
			ConstraintViolationException, VersionException,
			InvalidSerializedDataException, LockException, RepositoryException {
		session.importXML(parentAbsPath, in, uuidBehavior);
	}

	@Override
	public boolean isLive() {
		return session.isLive();
	}

	@Override
	public boolean itemExists(String absPath) throws RepositoryException {
		return session.itemExists(absPath);
	}

	@Override
	public void logout() {
		session.logout();
	}

	@Override
	public void move(String srcAbsPath, String destAbsPath)
			throws ItemExistsException, PathNotFoundException,
			VersionException, ConstraintViolationException, LockException,
			RepositoryException {
		session.move(srcAbsPath, destAbsPath);
	}

	@Override
	public boolean nodeExists(String absPath) throws RepositoryException {
		return session.nodeExists(absPath);
	}

	@Override
	public boolean propertyExists(String absPath) throws RepositoryException {
		return session.propertyExists(absPath);
	}

	@Override
	public void refresh(boolean keepChanges) throws RepositoryException {
		session.refresh(keepChanges);
	}

	@Override
	public void removeItem(String absPath) throws VersionException,
			LockException, ConstraintViolationException, AccessDeniedException,
			RepositoryException {
		session.removeItem(absPath);
	}

	@SuppressWarnings("deprecation")
	@Override
	public void removeLockToken(String lt) {
		session.removeLockToken(lt);
	}

	@Override
	public void save() throws AccessDeniedException, ItemExistsException,
			ReferentialIntegrityException, ConstraintViolationException,
			InvalidItemStateException, VersionException, LockException,
			NoSuchNodeTypeException, RepositoryException {
		session.save();
	}

	@Override
	public void setNamespacePrefix(String prefix, String uri)
			throws NamespaceException, RepositoryException {
		session.setNamespacePrefix(prefix, uri);
	}

}
