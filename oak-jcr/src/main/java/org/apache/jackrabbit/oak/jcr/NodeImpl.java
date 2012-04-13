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
package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.PropertyIteratorAdapter;
import org.apache.jackrabbit.oak.api.NodeStateEditor;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.TransientNodeState;
import org.apache.jackrabbit.oak.jcr.util.ItemNameMatcher;
import org.apache.jackrabbit.oak.jcr.util.LogUtil;
import org.apache.jackrabbit.oak.jcr.util.ValueConverter;
import org.apache.jackrabbit.oak.kernel.ScalarImpl;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;
import org.apache.jackrabbit.oak.util.Predicate;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.ItemVisitor;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.lock.Lock;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Iterator;

import static org.apache.jackrabbit.oak.util.Iterators.filter;

/**
 * {@code NodeImpl}...
 */
public class NodeImpl extends ItemImpl implements Node  {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NodeImpl.class);

    private TransientNodeState transientNodeState;

    NodeImpl(SessionContext<SessionImpl> sessionContext, TransientNodeState transientNodeState) {
        super(sessionContext);
        this.transientNodeState = transientNodeState;
    }

    //---------------------------------------------------------------< Item >---
    /**
     * @see javax.jcr.Item#isNode()
     */
    @Override
    public boolean isNode() {
        return true;
    }

    /**
     * @see javax.jcr.Item#getName()
     */
    @Override
    public String getName() throws RepositoryException {
        return getTransientNodeState().getName();
    }

    /**
     * @see javax.jcr.Item#getPath()
     */
    @Override
    public String getPath() throws RepositoryException {
        return '/' + path();
    }

    /**
     * @see javax.jcr.Item#getParent()
     */
    @Override
    public Node getParent() throws RepositoryException {
        if (getTransientNodeState().getParent() == null) {
            throw new ItemNotFoundException("Root has no parent");
        }

        return new NodeImpl(sessionContext, getTransientNodeState().getParent());
    }

    /**
     * @see Item#getAncestor(int)
     */
    @Override
    public Item getAncestor(int depth) throws RepositoryException {
        // todo implement getAncestor
        return null;
    }

    /**
     * @see javax.jcr.Item#getDepth()
     */
    @Override
    public int getDepth() throws RepositoryException {
        return Paths.getDepth(path());
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        // todo implement isNew
        return false;
    }

    /**
     * @see javax.jcr.Item#isModified()
     */
    @Override
    public boolean isModified() {
        // todo implement isModified
        return false;
    }

    /**
     * @see javax.jcr.Item#remove()
     */
    @Override
    public void remove() throws RepositoryException {
        getTransientNodeState().getParent().getEditor().removeNode(getName());
    }

    /**
     * @see Item#accept(javax.jcr.ItemVisitor)
     */
    @Override
    public void accept(ItemVisitor visitor) throws RepositoryException {
        checkStatus();
        visitor.visit(this);
    }

    //---------------------------------------------------------------< Node >---
    /**
     * @see Node#addNode(String)
     */
    @Override
    public Node addNode(String relPath) throws RepositoryException {
        checkStatus();

        String parentPath = Paths.concat(path(), Paths.getParentPath(relPath));
        TransientNodeState parentState = getItemStateProvider().getTransientNodeState(parentPath);
        if (parentState == null) {
            throw new PathNotFoundException(relPath);
        }

        String name = Paths.getName(relPath);
        parentState.getEditor().addNode(name);
        return new NodeImpl(sessionContext, parentState.getChildNode(name));
    }

    @Override
    public Node addNode(String relPath, String primaryNodeTypeName) throws RepositoryException {
        checkStatus();
        Node childNode = addNode(relPath);
        childNode.setPrimaryType(primaryNodeTypeName);
        return childNode;
    }

    @Override
    public void orderBefore(String srcChildRelPath, String destChildRelPath) throws RepositoryException {
        checkStatus();

        // TODO
    }

    /**
     * @see Node#setProperty(String, javax.jcr.Value)
     */
    @Override
    public Property setProperty(String name, Value value) throws RepositoryException {
        int type = PropertyType.UNDEFINED;
        if (value != null) {
            type = value.getType();
        }
        return setProperty(name, value, type);
    }

    /**
     * @see Node#setProperty(String, javax.jcr.Value, int)
     */
    @Override
    public Property setProperty(String name, Value value, int type) throws RepositoryException {
        checkStatus();

        getEditor().setProperty(name, ValueConverter.toScalar(value));
        return getProperty(name);
    }

    /**
     * @see Node#setProperty(String, javax.jcr.Value[])
     */
    @Override
    public Property setProperty(String name, Value[] values) throws RepositoryException {
        int type;
        if (values == null || values.length == 0 || values[0] == null) {
            type = PropertyType.UNDEFINED;
        } else {
            type = values[0].getType();
        }
        return setProperty(name, values, type);
    }

    @Override
    public Property setProperty(String name, Value[] values, int type) throws RepositoryException {
        checkStatus();

        getEditor().setProperty(name, ValueConverter.toScalar(values));
        return getProperty(name);
    }

    /**
     * @see Node#setProperty(String, String[])
     */
    @Override
    public Property setProperty(String name, String[] values) throws RepositoryException {
        return setProperty(name, values, PropertyType.UNDEFINED);
    }

    /**
     * @see Node#setProperty(String, String[], int)
     */
    @Override
    public Property setProperty(String name, String[] values, int type) throws RepositoryException {
        Value[] vs;
        if (type == PropertyType.UNDEFINED) {
            vs = ValueHelper.convert(values, PropertyType.STRING, getValueFactory());
        } else {
            vs = ValueHelper.convert(values, type, getValueFactory());
        }
        return setProperty(name, vs, type);
    }

    /**
     * @see Node#setProperty(String, String)
     */
    @Override
    public Property setProperty(String name, String value) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value, PropertyType.STRING);
        return setProperty(name, v, PropertyType.UNDEFINED);
    }

    /**
     * @see Node#setProperty(String, String, int)
     */
    @Override
    public Property setProperty(String name, String value, int type) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value, type);
        return setProperty(name, v, type);
    }

    /**
     * @see Node#setProperty(String, InputStream)
     */
    @SuppressWarnings("deprecation")
    @Override
    public Property setProperty(String name, InputStream value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.BINARY);
    }

    /**
     * @see Node#setProperty(String, Binary)
     */
    @Override
    public Property setProperty(String name, Binary value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.BINARY);
    }

    /**
     * @see Node#setProperty(String, boolean)
     */
    @Override
    public Property setProperty(String name, boolean value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.BOOLEAN);

    }

    /**
     * @see Node#setProperty(String, double)
     */
    @Override
    public Property setProperty(String name, double value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.DOUBLE);
    }

    /**
     * @see Node#setProperty(String, BigDecimal)
     */
    @Override
    public Property setProperty(String name, BigDecimal value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.DECIMAL);
    }

    /**
     * @see Node#setProperty(String, long)
     */
    @Override
    public Property setProperty(String name, long value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.LONG);
    }

    /**
     * @see Node#setProperty(String, Calendar)
     */
    @Override
    public Property setProperty(String name, Calendar value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.DATE);
    }

    /**
     * @see Node#setProperty(String, Node)
     */
    @Override
    public Property setProperty(String name, Node value) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value);
        return setProperty(name, v, PropertyType.REFERENCE);
    }

    @Override
    public Node getNode(String relPath) throws RepositoryException {
        checkStatus();

        NodeImpl node = getNodeOrNull(relPath);
        if (node == null) {
            throw new PathNotFoundException(relPath);
        }
        else {
            return node;
        }
    }

    @Override
    public NodeIterator getNodes() throws RepositoryException {
        checkStatus();

        Iterable<TransientNodeState> childNodeStates = getTransientNodeState().getChildNodes();
        return new NodeIteratorAdapter(nodeIterator(childNodeStates.iterator()));
    }

    @Override
    public NodeIterator getNodes(final String namePattern) throws RepositoryException {
        checkStatus();

        Iterator<TransientNodeState> childNodeStates = filter(getTransientNodeState().getChildNodes().iterator(),
                new Predicate<TransientNodeState>() {
                    @Override
                    public boolean evaluate(TransientNodeState state) {
                        return ItemNameMatcher.matches(state.getName(), namePattern);
                    }
                });

        return new NodeIteratorAdapter(nodeIterator(childNodeStates));
    }

    @Override
    public NodeIterator getNodes(final String[] nameGlobs) throws RepositoryException {
        checkStatus();

        Iterator<TransientNodeState> childNodeStates = filter(getTransientNodeState().getChildNodes().iterator(),
                new Predicate<TransientNodeState>() {
                    @Override
                    public boolean evaluate(TransientNodeState state) {
                        return ItemNameMatcher.matches(state.getName(), nameGlobs);
                    }
                });

        return new NodeIteratorAdapter(nodeIterator(childNodeStates));
    }

    @Override
    public Property getProperty(String relPath) throws RepositoryException {
        checkStatus();

        Property property = getPropertyOrNull(relPath);
        if (property == null) {
            throw new PathNotFoundException(relPath);
        } else {
            return property;
        }
    }

    @Override
    public PropertyIterator getProperties() throws RepositoryException {
        checkStatus();

        Iterable<PropertyState> properties = getTransientNodeState().getProperties();
        return new PropertyIteratorAdapter(propertyIterator(properties.iterator()));
    }

    @Override
    public PropertyIterator getProperties(final String namePattern) throws RepositoryException {
        checkStatus();

        Iterator<PropertyState> properties = filter(getTransientNodeState().getProperties().iterator(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean evaluate(PropertyState entry) {
                        return ItemNameMatcher.matches(entry.getName(), namePattern);
                    }
                });

        return new PropertyIteratorAdapter(propertyIterator(properties));
    }

    @Override
    public PropertyIterator getProperties(final String[] nameGlobs) throws RepositoryException {
        Iterator<PropertyState> propertyNames = filter(getTransientNodeState().getProperties().iterator(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean evaluate(PropertyState entry) {
                        return ItemNameMatcher.matches(entry.getName(), nameGlobs);
                    }
                });

        return new PropertyIteratorAdapter(propertyIterator(propertyNames));
    }

    /**
     * @see javax.jcr.Node#getPrimaryItem()
     */
    @Override
    public Item getPrimaryItem() throws RepositoryException {
        checkStatus();
        String name = getPrimaryNodeType().getPrimaryItemName();
        if (name == null) {
            throw new ItemNotFoundException("No primary item present on node " + LogUtil.safeGetJCRPath(this));
        }
        if (hasProperty(name)) {
            return getProperty(name);
        } else if (hasNode(name)) {
            return getNode(name);
        } else {
            throw new ItemNotFoundException("Primary item " + name + " does not exist on node " + LogUtil.safeGetJCRPath(this));
        }
    }

    /**
     * @see javax.jcr.Node#getUUID()
     */
    @Override
    public String getUUID() throws RepositoryException {
        checkStatus();

        if (hasProperty(JcrConstants.JCR_UUID) && isNodeType(JcrConstants.MIX_REFERENCEABLE)) {
            return getProperty(JcrConstants.JCR_UUID).getString();
        }

        throw new UnsupportedRepositoryOperationException("Node is not referenceable.");
    }

    @Override
    public String getIdentifier() throws RepositoryException {
        checkStatus();

        // TODO
        return path();
    }

    @Override
    public int getIndex() throws RepositoryException {
        // TODO
        return 0;
    }

    /**
     * @see javax.jcr.Node#getReferences()
     */
    @Override
    public PropertyIterator getReferences() throws RepositoryException {
        return getReferences(null);
    }

    @Override
    public PropertyIterator getReferences(String name) throws RepositoryException {
        checkStatus();

        // TODO
        return null;
    }

    /**
     * @see javax.jcr.Node#getWeakReferences()
     */
    @Override
    public PropertyIterator getWeakReferences() throws RepositoryException {
        return getWeakReferences(null);
    }

    @Override
    public PropertyIterator getWeakReferences(String name) throws RepositoryException {
        checkStatus();

        // TODO
        return null;
    }

    @Override
    public boolean hasNode(String relPath) throws RepositoryException {
        checkStatus();

        return getNodeOrNull(relPath) != null;
    }

    @Override
    public boolean hasProperty(String relPath) throws RepositoryException {
        checkStatus();

        return getPropertyOrNull(relPath) != null;
    }

    @Override
    public boolean hasNodes() throws RepositoryException {
        checkStatus();

        return getTransientNodeState().getChildNodeCount() != 0;
    }

    @Override
    public boolean hasProperties() throws RepositoryException {
        checkStatus();

        return getTransientNodeState().getPropertyCount() != 0;
    }

    @Override
    public NodeType getPrimaryNodeType() throws RepositoryException {
        checkStatus();

        // TODO
        return null;
    }

    @Override
    public NodeType[] getMixinNodeTypes() throws RepositoryException {
        checkStatus();

        // TODO
        return new NodeType[0];
    }

    @Override
    public boolean isNodeType(String nodeTypeName) throws RepositoryException {
        checkStatus();

        if (getProperty(JcrConstants.JCR_PRIMARYTYPE).getString().equals(nodeTypeName)) {
            return true;
        }
        if (hasProperty(JcrConstants.JCR_MIXINTYPES)) {
            Value[] mixins = getProperty(JcrConstants.JCR_MIXINTYPES).getValues();
            for (Value mixin : mixins) {
                if (mixin.getString().equals(nodeTypeName)) {
                    return true;
                }
            }
        }

        // TODO evaluate effective node type
        return false;
    }

    @Override
    public void setPrimaryType(String nodeTypeName) throws RepositoryException {
        checkStatus();

        getEditor().setProperty(JcrConstants.JCR_PRIMARYTYPE, ScalarImpl.stringScalar(nodeTypeName));
    }

    @Override
    public void addMixin(String mixinName) throws RepositoryException {
        checkStatus();

        // todo implement addMixin
    }

    @Override
    public void removeMixin(String mixinName) throws RepositoryException {
        checkStatus();

        // todo implement removeMixin
    }

    @Override
    public boolean canAddMixin(String mixinName) throws RepositoryException {
        // TODO
        return false;
    }

    @Override
    public NodeDefinition getDefinition() throws RepositoryException {
        checkStatus();

        // TODO
        return null;
    }


    @Override
    public String getCorrespondingNodePath(String workspaceName) throws RepositoryException {
        checkStatus();

        // TODO
        return null;
    }


    @Override
    public void update(String srcWorkspace) throws RepositoryException {
        checkStatus();
        ensureNoPendingSessionChanges();

        // TODO
    }

    /**
     * @see javax.jcr.Node#checkin()
     */
    @Override
    public Version checkin() throws RepositoryException {
        return getVersionManager().checkin(getPath());
    }

    /**
     * @see javax.jcr.Node#checkout()
     */
    @Override
    public void checkout() throws RepositoryException {
        getVersionManager().checkout(getPath());
    }

    /**
     * @see javax.jcr.Node#doneMerge(javax.jcr.version.Version)
     */
    @Override
    public void doneMerge(Version version) throws RepositoryException {
        getVersionManager().doneMerge(getPath(), version);
    }

    /**
     * @see javax.jcr.Node#cancelMerge(javax.jcr.version.Version)
     */
    @Override
    public void cancelMerge(Version version) throws RepositoryException {
        getVersionManager().cancelMerge(getPath(), version);
    }

    /**
     * @see javax.jcr.Node#merge(String, boolean)
     */
    @Override
    public NodeIterator merge(String srcWorkspace, boolean bestEffort) throws RepositoryException {
        return getVersionManager().merge(getPath(), srcWorkspace, bestEffort);
    }

    /**
     * @see javax.jcr.Node#isCheckedOut()
     */
    @Override
    public boolean isCheckedOut() throws RepositoryException {
        return getVersionManager().isCheckedOut(getPath());
    }

    /**
     * @see javax.jcr.Node#restore(String, boolean)
     */
    @Override
    public void restore(String versionName, boolean removeExisting) throws RepositoryException {
        getVersionManager().restore(getPath(), versionName, removeExisting);
    }

    /**
     * @see javax.jcr.Node#restore(javax.jcr.version.Version, boolean)
     */
    @Override
    public void restore(Version version, boolean removeExisting) throws RepositoryException {
        getVersionManager().restore(version, removeExisting);
    }

    /**
     * @see javax.jcr.Node#restore(Version, String, boolean)
     */
    @Override
    public void restore(Version version, String relPath, boolean removeExisting) throws RepositoryException {
        // additional checks are performed with subsequent calls.
        if (hasNode(relPath)) {
            // node at 'relPath' exists -> call restore on the target Node
            getNode(relPath).restore(version, removeExisting);
        } else {
            // TODO
        }
    }

    /**
     * @see javax.jcr.Node#restoreByLabel(String, boolean)
     */
    @Override
    public void restoreByLabel(String versionLabel, boolean removeExisting) throws RepositoryException {
        getVersionManager().restoreByLabel(getPath(), versionLabel, removeExisting);
    }

    /**
     * @see javax.jcr.Node#getVersionHistory()
     */
    @Override
    public VersionHistory getVersionHistory() throws RepositoryException {
        return getVersionManager().getVersionHistory(getPath());
    }

    /**
     * @see javax.jcr.Node#getBaseVersion()
     */
    @Override
    public Version getBaseVersion() throws RepositoryException {
        return getVersionManager().getBaseVersion(getPath());
    }

    /**
     * @see javax.jcr.Node#lock(boolean, boolean)
     */
    @Override
    public Lock lock(boolean isDeep, boolean isSessionScoped) throws RepositoryException {
        return getLockManager().lock(getPath(), isDeep, isSessionScoped, Long.MAX_VALUE, null);
    }

    /**
     * @see javax.jcr.Node#getLock()
     */
    @Override
    public Lock getLock() throws RepositoryException {
        return getLockManager().getLock(getPath());
    }

    /**
     * @see javax.jcr.Node#unlock()
     */
    @Override
    public void unlock() throws RepositoryException {
        getLockManager().unlock(getPath());
    }

    /**
     * @see javax.jcr.Node#holdsLock()
     */
    @Override
    public boolean holdsLock() throws RepositoryException {
        return getLockManager().holdsLock(getPath());
    }

    /**
     * @see javax.jcr.Node#isLocked() ()
     */
    @Override
    public boolean isLocked() throws RepositoryException {
        return getLockManager().isLocked(getPath());
    }


    @Override
    public NodeIterator getSharedSet() throws RepositoryException {
        // TODO
        return null;
    }

    @Override
    public void removeSharedSet() throws RepositoryException {
        // TODO
    }

    @Override
    public void removeShare() throws RepositoryException {
        // TODO
    }

    /**
     * @see javax.jcr.Node#followLifecycleTransition(String)
     */
    @Override
    public void followLifecycleTransition(String transition) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Lifecycle Management is not supported");
    }

    /**
     * @see javax.jcr.Node#getAllowedLifecycleTransistions()
     */
    @Override
    public String[] getAllowedLifecycleTransistions() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Lifecycle Management is not supported");

    }

    //--------------------------------------------------------------------------
    /**
     * Access to KernelNodeStateEditor to allow code in other packages to
     * access item states.
     *
     * @return The node state editor.
     */
    public NodeStateEditor getEditor() {
        return getTransientNodeState().getEditor();
    }

    //------------------------------------------------------------< private >---
    /**
     * Shortcut to retrieve the version manager from the workspace associated
     * with the editing session.
     *
     * @return the version manager associated with the editing session.
     * @throws RepositoryException If an error occurs while retrieving the version manager.
     */
    private VersionManager getVersionManager() throws RepositoryException {
        return getSession().getWorkspace().getVersionManager();
    }

    /**
     * Shortcut to retrieve the lock manager from the workspace associated
     * with the editing session.
     *
     * @return the lock manager associated with the editing session.
     * @throws RepositoryException If an error occurs while retrieving the lock manager.
     */
    private LockManager getLockManager() throws RepositoryException {
        return getSession().getWorkspace().getLockManager();
    }

    private ItemStateProvider getItemStateProvider() {
        return sessionContext.getItemStateProvider();
    }
    
    private synchronized TransientNodeState getTransientNodeState() {
        return transientNodeState = getItemStateProvider().getTransientNodeState(transientNodeState.getPath());
    }

    private String path() {
        return getTransientNodeState().getPath();
    }

    private Iterator<Node> nodeIterator(Iterator<TransientNodeState> childNodeStates) {
        return Iterators.map(childNodeStates, new Function1<TransientNodeState, Node>() {
            @Override
            public Node apply(TransientNodeState state) {
                return new NodeImpl(sessionContext, state);
            }
        });
    }

    private Iterator<Property> propertyIterator(Iterator<PropertyState> properties) {
        return Iterators.map(properties, new Function1<PropertyState, Property>() {
            @Override
            public Property apply(PropertyState propertyState) {
                return new PropertyImpl(sessionContext, getTransientNodeState(), propertyState);
            }
        });
    }

    private NodeImpl getNodeOrNull(String relPath) {
        String absPath = Paths.concat(path(), relPath);
        TransientNodeState nodeState = getItemStateProvider().getTransientNodeState(absPath);
        return nodeState == null
            ? null
            : new NodeImpl(sessionContext, nodeState);
    }
    
    private PropertyImpl getPropertyOrNull(String relPath) {
        String absPath = Paths.concat(path(), Paths.getParentPath(relPath));
        TransientNodeState parentState = getItemStateProvider().getTransientNodeState(absPath);
        if (parentState == null) {
            return null;
        }

        String name = Paths.getName(relPath);
        PropertyState propertyState = parentState.getProperty(name);
        return propertyState == null
            ? null
            : new PropertyImpl(sessionContext, parentState, propertyState);
    }
}