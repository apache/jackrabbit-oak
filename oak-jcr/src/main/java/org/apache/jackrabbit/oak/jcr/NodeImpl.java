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
import org.apache.jackrabbit.oak.jcr.SessionImpl.Context;
import org.apache.jackrabbit.oak.jcr.json.JsonValue;
import org.apache.jackrabbit.oak.jcr.json.JsonValue.JsonAtom;
import org.apache.jackrabbit.oak.jcr.state.PropertyStateImpl;
import org.apache.jackrabbit.oak.jcr.state.TransientNodeState;
import org.apache.jackrabbit.oak.jcr.util.Function1;
import org.apache.jackrabbit.oak.jcr.util.ItemNameMatcher;
import org.apache.jackrabbit.oak.jcr.util.Iterators;
import org.apache.jackrabbit.oak.jcr.util.LogUtil;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.apache.jackrabbit.oak.jcr.util.Predicate;
import org.apache.jackrabbit.oak.jcr.util.ValueConverter;
import org.apache.jackrabbit.mk.model.PropertyState;
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
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Iterator;

import static org.apache.jackrabbit.oak.jcr.util.Iterators.filter;

/**
 * {@code NodeImpl}...
 */
public class NodeImpl extends ItemImpl implements Node  {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NodeImpl.class);

    private final TransientNodeState state;

    static boolean exist(Context sessionContext, Path path) {
        return getNodeState(sessionContext, path) != null;
    }

    static Node create(Context sessionContext, Path path) throws PathNotFoundException {
        TransientNodeState state = getNodeState(sessionContext, path);
        if (state == null) {
            throw new PathNotFoundException(path.toJcrPath());
        }

        return new NodeImpl(sessionContext, state);
    }

    static Node create(Context sessionContext, TransientNodeState state) {
        return new NodeImpl(sessionContext, state);
    }

    private NodeImpl(Context sessionContext, TransientNodeState state) {
        super(sessionContext);
        this.state = state;
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
        return state.getName();
    }

    /**
     * @see javax.jcr.Item#getPath()
     */
    @Override
    public String getPath() throws RepositoryException {
        return path().toJcrPath();
    }

    /**
     * @see javax.jcr.Item#getParent()
     */
    @Override
    public Node getParent() throws RepositoryException {
        if (state.isRoot()) {
            throw new ItemNotFoundException("Root has no parent");
        }

        return create(sessionContext, path().getParent());
    }

    /**
     * @see Item#getAncestor(int)
     */
    @Override
    public Item getAncestor(int depth) throws RepositoryException {
        Path parent = path().getAncestor(depth);
        if (parent == null) {
            throw new ItemNotFoundException(path().toJcrPath() + "has no ancestor of depth " + depth);
        }

        return create(sessionContext, parent);
    }

    /**
     * @see javax.jcr.Item#getDepth()
     */
    @Override
    public int getDepth() throws RepositoryException {
        return path().getDepth();
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        return state.isNew();
    }

    /**
     * @see javax.jcr.Item#isModified()
     */
    @Override
    public boolean isModified() {
        return state.isModified();
    }

    /**
     * @see javax.jcr.Item#remove()
     */
    @Override
    public void remove() throws RepositoryException {
        state.remove();
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
        Path newPath = path().concat(relPath);
        TransientNodeState parentState = getNodeState(sessionContext, newPath.getParent());
        TransientNodeState childState = parentState.addNode(newPath.getName());
        return create(sessionContext, childState);
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

        state.setProperty(name, ValueConverter.toJsonValue(value));
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

        state.setProperty(name, ValueConverter.toJsonValue(values));
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
        return create(sessionContext, path().concat(relPath));
    }

    @Override
    public NodeIterator getNodes() throws RepositoryException {
        checkStatus();

        Iterator<TransientNodeState> childNodeStates = state.getNodes();
        return new NodeIteratorAdapter(nodeIterator(childNodeStates));
    }

    @Override
    public NodeIterator getNodes(final String namePattern) throws RepositoryException {
        checkStatus();

        Iterator<TransientNodeState> childNodeStates = filter(state.getNodes(),
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

        Iterator<TransientNodeState> childNodeStates = filter(state.getNodes(),
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

        return PropertyImpl.create(sessionContext, path().concat(relPath));
    }

    @Override
    public PropertyIterator getProperties() throws RepositoryException {
        checkStatus();

        Iterator<PropertyState> properties = state.getProperties();
        return new PropertyIteratorAdapter(propertyIterator(properties));
    }

    @Override
    public PropertyIterator getProperties(final String namePattern) throws RepositoryException {
        checkStatus();

        Iterator<PropertyState> properties = filter(state.getProperties(),
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
        Iterator<PropertyState> propertyNames = filter(state.getProperties(),
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
        return path().toMkPath();
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

        return exist(sessionContext, path().concat(relPath));
    }

    @Override
    public boolean hasProperty(String relPath) throws RepositoryException {
        checkStatus();

        return PropertyImpl.exist(sessionContext, path().concat(relPath));
    }

    @Override
    public boolean hasNodes() throws RepositoryException {
        checkStatus();

        return state.hasNodes();
    }

    @Override
    public boolean hasProperties() throws RepositoryException {
        checkStatus();

        return state.hasProperties();
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

        // TODO
        return false;
    }

    @Override
    public void setPrimaryType(String nodeTypeName) throws RepositoryException {
        checkStatus();

        state.setProperty(JcrConstants.JCR_PRIMARYTYPE, JsonAtom.string(nodeTypeName));
    }

    @Override
    public void addMixin(String mixinName) throws RepositoryException {
        checkStatus();

        JsonValue mixins = state.getPropertyValue(JcrConstants.JCR_MIXINTYPES);
        mixins.asArray().add(JsonAtom.string(mixinName));
        state.setProperty(JcrConstants.JCR_MIXINTYPES, mixins);
    }

    @Override
    public void removeMixin(String mixinName) throws RepositoryException {
        checkStatus();

        JsonValue mixins = state.getPropertyValue(JcrConstants.JCR_MIXINTYPES);
        mixins.asArray().remove(JsonAtom.string(mixinName));
        state.setProperty(JcrConstants.JCR_MIXINTYPES, mixins);
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
        checkSessionHasPendingChanges();

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
        return getSession().getWorkspace().getVersionManager().isCheckedOut(getPath());
    }

    /**
     * @see javax.jcr.Node#restore(String, boolean)
     */
    @Override
    public void restore(String versionName, boolean removeExisting) throws RepositoryException {
        getSession().getWorkspace().getVersionManager().restore(getPath(), versionName, removeExisting);
    }

    /**
     * @see javax.jcr.Node#restore(javax.jcr.version.Version, boolean)
     */
    @Override
    public void restore(Version version, boolean removeExisting) throws RepositoryException {
        getSession().getWorkspace().getVersionManager().restore(version, removeExisting);
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
        return getSession().getWorkspace().getLockManager().lock(getPath(), isDeep, isSessionScoped, Long.MAX_VALUE, null);
    }

    /**
     * @see javax.jcr.Node#getLock()
     */
    @Override
    public Lock getLock() throws RepositoryException {
        return getSession().getWorkspace().getLockManager().getLock(getPath());
    }

    /**
     * @see javax.jcr.Node#unlock()
     */
    @Override
    public void unlock() throws RepositoryException {
        getSession().getWorkspace().getLockManager().unlock(getPath());
    }

    /**
     * @see javax.jcr.Node#holdsLock()
     */
    @Override
    public boolean holdsLock() throws RepositoryException {
        return getSession().getWorkspace().getLockManager().holdsLock(getPath());
    }

    /**
     * @see javax.jcr.Node#isLocked() ()
     */
    @Override
    public boolean isLocked() throws RepositoryException {
        return getSession().getWorkspace().getLockManager().isLocked(getPath());
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

    private Path path() {
        return state.getPath();
    }

    private Iterator<Node> nodeIterator(Iterator<TransientNodeState> childNodeStates) {
        return Iterators.map(childNodeStates, new Function1<TransientNodeState, Node>() {
            @Override
            public Node apply(TransientNodeState state) {
                return NodeImpl.create(sessionContext, state);
            }
        });
    }

    private Iterator<Property> propertyIterator(Iterator<PropertyState> properties) {
        return Iterators.map(properties, new Function1<PropertyState, Property>() {
            @Override
            public Property apply(PropertyState state) { // fixme don't cast, see OAK-16
                JsonValue value = ((PropertyStateImpl) state).getValue();
                return PropertyImpl.create(sessionContext, NodeImpl.this.state, state.getName(), value);
            }
        });
    }
}