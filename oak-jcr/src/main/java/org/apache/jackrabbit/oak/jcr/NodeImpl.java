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

import static org.apache.jackrabbit.oak.util.Iterators.filter;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

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
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.version.OnParentVersionAction;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.PropertyIteratorAdapter;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.jcr.util.ItemNameMatcher;
import org.apache.jackrabbit.oak.jcr.util.LogUtil;
import org.apache.jackrabbit.oak.jcr.util.ValueConverter;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;
import org.apache.jackrabbit.oak.util.Predicate;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code NodeImpl}...
 */
public class NodeImpl extends ItemImpl implements Node  {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NodeImpl.class);

    private final NodeDelegate dlg;
    
    NodeImpl(NodeDelegate dlg) {
        super(dlg.getSessionContext(), dlg);
        this.dlg = dlg;
    }

    // TODO
    public String getOakPath() {
        return dlg.getPath();
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
     * @see javax.jcr.Item#getParent()
     */
    @Override
    public Node getParent() throws RepositoryException {
        return new NodeImpl(dlg.getParent());
    }

    /**
     * @see Item#getAncestor(int)
     */
    @Override
    public Item getAncestor(int depth) throws RepositoryException {
        return new NodeImpl(dlg.getAncestor(depth));
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        return dlg.getNodeStatus() == Status.NEW;
    }

    /**
     * @see javax.jcr.Item#isModified()
     */
    @Override
    public boolean isModified() {
        return dlg.getNodeStatus() == Status.MODIFIED;
    }

    /**
     * @see javax.jcr.Item#remove()
     */
    @Override
    public void remove() throws RepositoryException {
        dlg.remove();
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
    public Node addNode(String relJcrPath) throws RepositoryException {
        checkStatus();
        NodeDelegate added = dlg.addNode(toOakPath(relJcrPath));
        return new NodeImpl(added);
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
        throw new UnsupportedRepositoryOperationException("TODO: ordering not supported");
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
    public Property setProperty(String jcrName, Value value, int type)
            throws RepositoryException {
        checkStatus();

        int targetType = getTargetType(value, type);
        Value targetValue = ValueHelper.convert(value, targetType,
                getValueFactory());
        CoreValue oakValue = ValueConverter.toCoreValue(targetValue,
                sessionContext);
        return new PropertyImpl(dlg.setProperty(toOakPath(jcrName), oakValue));
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
    public Property setProperty(String jcrName, Value[] values, int type)
            throws RepositoryException {
        checkStatus();

        int targetType = getTargetType(values, type);
        Value[] targetValue = ValueHelper.convert(values, targetType,
                getValueFactory());
        List<CoreValue> oakValue = ValueConverter.toCoreValues(targetValue,
                sessionContext);
        return new PropertyImpl(dlg.setProperty(toOakPath(jcrName), oakValue));
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

        Iterator<NodeDelegate> children = dlg.getChildren();
        return new NodeIteratorAdapter(nodeIterator(children));
    }

    @Override
    public NodeIterator getNodes(final String namePattern)
            throws RepositoryException {
        checkStatus();

        Iterator<NodeDelegate> children = filter(dlg.getChildren(),
                new Predicate<NodeDelegate>() {
                    @Override
                    public boolean evaluate(NodeDelegate state) {
                        return ItemNameMatcher.matches(
                                toJcrPath(state.getName()), namePattern);
                    }
                });

        return new NodeIteratorAdapter(nodeIterator(children));
    }

    @Override
    public NodeIterator getNodes(final String[] nameGlobs) throws RepositoryException {
        checkStatus();

        Iterator<NodeDelegate> children = filter(dlg.getChildren(),
                new Predicate<NodeDelegate>() {
                    @Override
                    public boolean evaluate(NodeDelegate state) {
                        return ItemNameMatcher.matches(toJcrPath(state.getName()), nameGlobs);
                    }
                });

        return new NodeIteratorAdapter(nodeIterator(children));
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

        Iterator<PropertyDelegate> properties = dlg.getProperties();
        return new PropertyIteratorAdapter(propertyIterator(properties));
    }

    @Override
    public PropertyIterator getProperties(final String namePattern) throws RepositoryException {
        checkStatus();

        Iterator<PropertyDelegate> properties = filter(dlg.getProperties(),
                new Predicate<PropertyDelegate>() {
                    @Override
                    public boolean evaluate(PropertyDelegate entry) {
                        return ItemNameMatcher.matches(
                                toJcrPath(entry.getName()), namePattern);
                    }
                });

        return new PropertyIteratorAdapter(propertyIterator(properties));
    }

    @Override
    public PropertyIterator getProperties(final String[] nameGlobs) throws RepositoryException {
        Iterator<PropertyDelegate> propertyNames = filter(dlg.getProperties(),
                new Predicate<PropertyDelegate>() {
                    @Override
                    public boolean evaluate(PropertyDelegate entry) {
                        return ItemNameMatcher.matches(
                                toJcrPath(entry.getName()), nameGlobs);
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

        if (isNodeType(JcrConstants.MIX_REFERENCEABLE)) {
            return getProperty(JcrConstants.JCR_UUID).getString();
        } else {
            // TODO
            return getPath();
        }
    }

    @Override
    public int getIndex() throws RepositoryException {
        // as long as we do not support same name siblings, index always is 1
        return 1;
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
        throw new UnsupportedRepositoryOperationException("TODO: Node.getReferences");
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
        throw new UnsupportedRepositoryOperationException("TODO: Node.getWeakReferences");
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

        return dlg.getChildrenCount() != 0;
    }

    @Override
    public boolean hasProperties() throws RepositoryException {
        checkStatus();

        return dlg.getPropertyCount() != 0;
    }

    /**
     * @see javax.jcr.Node#getPrimaryNodeType()
     */
    @Override
    public NodeType getPrimaryNodeType() throws RepositoryException {
        checkStatus();

        // TODO: check if transient changes to mixin-types are reflected here
        NodeTypeManager ntMgr =
                getSession().getWorkspace().getNodeTypeManager();
        String primaryNtName = getProperty(JcrConstants.JCR_PRIMARYTYPE).getString();
        return ntMgr.getNodeType(primaryNtName);
    }

    /**
     * @see javax.jcr.Node#getMixinNodeTypes()
     */
    @Override
    public NodeType[] getMixinNodeTypes() throws RepositoryException {
        checkStatus();

        // TODO: check if transient changes to mixin-types are reflected here
        if (hasProperty(JcrConstants.JCR_MIXINTYPES)) {
            NodeTypeManager ntMgr =
                    getSession().getWorkspace().getNodeTypeManager();
            Value[] mixinNames = getProperty(JcrConstants.JCR_MIXINTYPES).getValues();
            NodeType[] mixinTypes = new NodeType[mixinNames.length];
            for (int i = 0; i < mixinNames.length; i++) {
                mixinTypes[i] = ntMgr.getNodeType(mixinNames[i].getString());
            }
            return mixinTypes;
        } else {
            return new NodeType[0];
        }
    }

    @Override
    public boolean isNodeType(String nodeTypeName) throws RepositoryException {
        checkStatus();

        if (hasProperty(JcrConstants.JCR_PRIMARYTYPE) && getProperty(JcrConstants.JCR_PRIMARYTYPE).getString().equals(nodeTypeName)) {
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

        CoreValue cv = ValueConverter.toCoreValue(nodeTypeName, PropertyType.NAME, sessionContext);
        dlg.setProperty(toOakPath(JcrConstants.JCR_PRIMARYTYPE), cv);
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
        return new NodeDefinition() {
 
            // This is a workaround to make AbstractJCRTest.cleanup happy
            
            @Override
            public boolean isProtected() {
                return false;
            }
            
            @Override
            public boolean isMandatory() {
                return false;
            }
            
            @Override
            public boolean isAutoCreated() {
                return false;
            }
            
            @Override
            public int getOnParentVersion() {
                return OnParentVersionAction.COPY;
            }
            
            @Override
            public String getName() {
                return "default";
            }
            
            @Override
            public NodeType getDeclaringNodeType() {
                return null;
            }
            
            @Override
            public NodeType[] getRequiredPrimaryTypes() {
                return null;
            }
            
            @Override
            public String[] getRequiredPrimaryTypeNames() {
                return null;
            }
            
            @Override
            public String getDefaultPrimaryTypeName() {
                return null;
            }
            
            @Override
            public NodeType getDefaultPrimaryType() {
                return null;
            }
            
            @Override
            public boolean allowsSameNameSiblings() {
                return false;
            }
        };
    }


    @Override
    public String getCorrespondingNodePath(String workspaceName) throws RepositoryException {
        checkStatus();
        throw new UnsupportedRepositoryOperationException("TODO: Node.getCorrespondingNodePath");
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
        return sessionContext.getVersionManager().checkin(getPath());
    }

    /**
     * @see javax.jcr.Node#checkout()
     */
    @Override
    public void checkout() throws RepositoryException {
        sessionContext.getVersionManager().checkout(getPath());
    }

    /**
     * @see javax.jcr.Node#doneMerge(javax.jcr.version.Version)
     */
    @Override
    public void doneMerge(Version version) throws RepositoryException {
        sessionContext.getVersionManager().doneMerge(getPath(), version);
    }

    /**
     * @see javax.jcr.Node#cancelMerge(javax.jcr.version.Version)
     */
    @Override
    public void cancelMerge(Version version) throws RepositoryException {
        sessionContext.getVersionManager().cancelMerge(getPath(), version);
    }

    /**
     * @see javax.jcr.Node#merge(String, boolean)
     */
    @Override
    public NodeIterator merge(String srcWorkspace, boolean bestEffort) throws RepositoryException {
        return sessionContext.getVersionManager().merge(getPath(), srcWorkspace, bestEffort);
    }

    /**
     * @see javax.jcr.Node#isCheckedOut()
     */
    @Override
    public boolean isCheckedOut() throws RepositoryException {
        return sessionContext.getVersionManager().isCheckedOut(getPath());
    }

    /**
     * @see javax.jcr.Node#restore(String, boolean)
     */
    @Override
    public void restore(String versionName, boolean removeExisting) throws RepositoryException {
        sessionContext.getVersionManager().restore(getPath(), versionName, removeExisting);
    }

    /**
     * @see javax.jcr.Node#restore(javax.jcr.version.Version, boolean)
     */
    @Override
    public void restore(Version version, boolean removeExisting) throws RepositoryException {
        sessionContext.getVersionManager().restore(version, removeExisting);
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
        sessionContext.getVersionManager().restoreByLabel(getPath(), versionLabel, removeExisting);
    }

    /**
     * @see javax.jcr.Node#getVersionHistory()
     */
    @Override
    public VersionHistory getVersionHistory() throws RepositoryException {
        return sessionContext.getVersionManager().getVersionHistory(getPath());
    }

    /**
     * @see javax.jcr.Node#getBaseVersion()
     */
    @Override
    public Version getBaseVersion() throws RepositoryException {
        return sessionContext.getVersionManager().getBaseVersion(getPath());
    }

    /**
     * @see javax.jcr.Node#lock(boolean, boolean)
     */
    @Override
    public Lock lock(boolean isDeep, boolean isSessionScoped) throws RepositoryException {
        return sessionContext.getLockManager().lock(getPath(), isDeep, isSessionScoped, Long.MAX_VALUE, null);
    }

    /**
     * @see javax.jcr.Node#getLock()
     */
    @Override
    public Lock getLock() throws RepositoryException {
        return sessionContext.getLockManager().getLock(getPath());
    }

    /**
     * @see javax.jcr.Node#unlock()
     */
    @Override
    public void unlock() throws RepositoryException {
        sessionContext.getLockManager().unlock(getPath());
    }

    /**
     * @see javax.jcr.Node#holdsLock()
     */
    @Override
    public boolean holdsLock() throws RepositoryException {
        return sessionContext.getLockManager().holdsLock(getPath());
    }

    /**
     * @see javax.jcr.Node#isLocked() ()
     */
    @Override
    public boolean isLocked() throws RepositoryException {
        return sessionContext.getLockManager().isLocked(getPath());
    }


    @Override
    public NodeIterator getSharedSet() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("TODO: Node.getSharedSet");
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


    private Iterator<Node> nodeIterator(Iterator<NodeDelegate> childNodes) {
        return Iterators.map(childNodes, new Function1<NodeDelegate, Node>() {
            @Override
            public Node apply(NodeDelegate state) {
                return new NodeImpl(state);
            }
        });
    }

    private Iterator<Property> propertyIterator(
            Iterator<PropertyDelegate> properties) {
        return Iterators.map(properties,
                new Function1<PropertyDelegate, Property>() {
                    @Override
                    public Property apply(PropertyDelegate propertyDelegate) {
                        return new PropertyImpl(propertyDelegate);
                    }
                });
    }

    private NodeImpl getNodeOrNull(String relJcrPath)
            throws RepositoryException {

        String relOakPath = toOakPath(relJcrPath);

        NodeDelegate nd = dlg.getNodeOrNull(relOakPath);
        return nd == null ? null : new NodeImpl(nd);
    }

    private PropertyImpl getPropertyOrNull(String relJcrPath)
            throws RepositoryException {

        String relOakPath = toOakPath(relJcrPath);

        PropertyDelegate pd = dlg.getPropertyOrNull(relOakPath);
        return pd == null ? null : new PropertyImpl(pd);
    }

    private int getTargetType(Value value, int type) {
        if (value == null) {
            return PropertyType.STRING; // TODO: review again. rather use
                                        // property definition
        } else {
            return value.getType();
        }
    }

    private int getTargetType(Value[] values, int type) {
        if (values == null || values.length == 0) {
            return PropertyType.STRING; // TODO: review again. rather use
                                        // property definition
        } else {
            // TODO deal with values array containing a null value in the first
            // position
            return getTargetType(values[0], type);
        }
    }
}