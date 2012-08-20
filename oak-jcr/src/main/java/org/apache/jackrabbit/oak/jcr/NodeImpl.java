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

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Item;
import javax.jcr.ItemExistsException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.ItemVisitor;
import javax.jcr.NoSuchWorkspaceException;
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
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.commons.ItemNameMatcher;
import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.PropertyIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.jcr.value.ValueConverter;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.util.TODO;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.jcr.Property.JCR_LOCK_IS_DEEP;
import static javax.jcr.Property.JCR_LOCK_OWNER;

/**
 * {@code NodeImpl}...
 */
public class NodeImpl extends ItemImpl<NodeDelegate> implements Node {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NodeImpl.class);

    public NodeImpl(NodeDelegate dlg) {
        super(dlg.getSessionDelegate(), dlg);
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
    @Nonnull
    public Node getParent() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeImpl>() {
            @Override
            public NodeImpl perform() throws RepositoryException {
                if (dlg.isRoot()) {
                    throw new ItemNotFoundException("Root has no parent");
                } else {
                    NodeDelegate parent = dlg.getParent();
                    if (parent == null) {
                        throw new AccessDeniedException();
                    }
                    return new NodeImpl(parent);
                }
            }
        });
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        try {
            return sessionDelegate.perform(new SessionOperation<Boolean>() {
                @Override
                public Boolean perform() throws InvalidItemStateException {
                    return !dlg.isStale() && dlg.getStatus() == Status.NEW;
                }
            });
        }
        catch (RepositoryException e) {
            return false;
        }
    }

    /**
     * @see javax.jcr.Item#isModified()
     */
    @Override
    public boolean isModified() {
        try {
            return sessionDelegate.perform(new SessionOperation<Boolean>() {
                @Override
                public Boolean perform() throws InvalidItemStateException {
                    return !dlg.isStale() && dlg.getStatus() == Status.MODIFIED;
                }
            });
        }
        catch (RepositoryException e) {
            return false;
        }
    }

    /**
     * @see javax.jcr.Item#remove()
     */
    @Override
    public void remove() throws RepositoryException {
        checkStatus();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                if (dlg.isRoot()) {
                    throw new RepositoryException("Cannot remove the root node");
                }

                dlg.remove();
                return null;
            }
        });
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
    @Nonnull
    public Node addNode(String relPath) throws RepositoryException {
        checkStatus();
        return addNode(relPath, null);
    }

    @Override
    @Nonnull
    public Node addNode(final String relPath, final String primaryNodeTypeName) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Node>() {
            @Override
            public Node perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathKeepIndexOrThrowNotFound(relPath);
                String oakName = PathUtils.getName(oakPath);
                String parentPath = sessionDelegate.getOakPathOrThrow(PathUtils.getParentPath(oakPath));

                // handle index
                if (oakName.contains("[")) {
                    throw new RepositoryException("Cannot create a new node using a name including an index");
                }

                NodeDelegate parent = dlg.getChild(parentPath);
                if (parent == null) {
                    // is it a property?
                    String grandParentPath = PathUtils.getParentPath(parentPath);
                    NodeDelegate grandParent = dlg.getChild(grandParentPath);
                    if (grandParent != null) {
                        String propName = PathUtils.getName(parentPath);
                        if (grandParent.getProperty(propName) != null) {
                            throw new ConstraintViolationException("Can't add new node to property.");
                        }
                    }

                    throw new PathNotFoundException(relPath);
                }

                if (parent.getChild(oakName) != null) {
                    throw new ItemExistsException(relPath);
                }

                // TODO retrieve matching nt from effective definition based on name-matching.
                String ntName = primaryNodeTypeName == null ? NodeType.NT_UNSTRUCTURED : primaryNodeTypeName;

                // TODO: figure out the right place for this check
                NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
                NodeType nt = ntm.getNodeType(ntName); // throws on not found
                if (nt.isAbstract() || nt.isMixin()) {
                    throw new ConstraintViolationException();
                }
                // TODO: END

                NodeDelegate added = parent.addChild(oakName);
                if (added == null) {
                    throw new ItemExistsException();
                }

                Node childNode = new NodeImpl(added);
                childNode.setPrimaryType(ntName);
                return childNode;
            }
        });
    }

    @Override
    public void orderBefore(final String srcChildRelPath, final String destChildRelPath) throws RepositoryException {
        checkStatus();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                String oakSrcChildRelPath =
                        sessionDelegate.getOakPathOrThrowNotFound(srcChildRelPath);
                String oakDestChildRelPath = null;
                if (destChildRelPath != null) {
                    oakDestChildRelPath =
                            sessionDelegate.getOakPathOrThrowNotFound(destChildRelPath);
                }
                dlg.orderBefore(oakSrcChildRelPath, oakDestChildRelPath);
                return null;
            }
        });
    }

    /**
     * @see Node#setProperty(String, javax.jcr.Value)
     */
    @Override
    @CheckForNull
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
    @CheckForNull
    public Property setProperty(final String jcrName, final Value value, final int type)
            throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyImpl>() {
            @Override
            public PropertyImpl perform() throws RepositoryException {
                String oakName = sessionDelegate.getOakPathOrThrow(jcrName);
                if (value == null) {
                    dlg.removeProperty(oakName);
                    return null;
                } else {
                    int targetType = getTargetType(value, type);
                    Value targetValue =
                            ValueHelper.convert(value, targetType, getValueFactory());
                    CoreValue oakValue =
                            ValueConverter.toCoreValue(targetValue, sessionDelegate);
                    return new PropertyImpl(dlg.setProperty(oakName, oakValue));
                }
            }
        });
    }

    /**
     * @see Node#setProperty(String, javax.jcr.Value[])
     */
    @Override
    @Nonnull
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
    @Nonnull
    public Property setProperty(final String jcrName, final Value[] values, final int type) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Property>() {
            @Override
            public Property perform() throws RepositoryException {
                int targetType = getTargetType(values, type);
                Value[] targetValues = ValueHelper.convert(values, targetType, getValueFactory());
                if (targetValues == null) {
                    Property p = getProperty(jcrName);
                    p.remove();
                    return p;
                } else {
                    String oakName = sessionDelegate.getOakPathOrThrow(jcrName);
                    List<CoreValue> oakValue = ValueConverter.toCoreValues(targetValues, sessionDelegate);
                    return new PropertyImpl(dlg.setProperty(oakName, oakValue));
                }
            }
        });
    }

    /**
     * @see Node#setProperty(String, String[])
     */
    @Override
    @Nonnull
    public Property setProperty(String name, String[] values) throws RepositoryException {
        return setProperty(name, values, PropertyType.UNDEFINED);
    }

    /**
     * @see Node#setProperty(String, String[], int)
     */
    @Override
    @Nonnull
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
    @CheckForNull
    public Property setProperty(String name, String value) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value, PropertyType.STRING);
        return setProperty(name, v, PropertyType.UNDEFINED);
    }

    /**
     * @see Node#setProperty(String, String, int)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, String value, int type) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value, type);
        return setProperty(name, v, type);
    }

    /**
     * @see Node#setProperty(String, InputStream)
     */
    @SuppressWarnings("deprecation")
    @Override
    @CheckForNull
    public Property setProperty(String name, InputStream value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.BINARY);
    }

    /**
     * @see Node#setProperty(String, Binary)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, Binary value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.BINARY);
    }

    /**
     * @see Node#setProperty(String, boolean)
     */
    @Override
    @Nonnull
    public Property setProperty(String name, boolean value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.BOOLEAN);
    }

    /**
     * @see Node#setProperty(String, double)
     */
    @Override
    @Nonnull
    public Property setProperty(String name, double value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.DOUBLE);
    }

    /**
     * @see Node#setProperty(String, BigDecimal)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, BigDecimal value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.DECIMAL);
    }

    /**
     * @see Node#setProperty(String, long)
     */
    @Override
    @Nonnull
    public Property setProperty(String name, long value) throws RepositoryException {
        return setProperty(name, getValueFactory().createValue(value), PropertyType.LONG);
    }

    /**
     * @see Node#setProperty(String, Calendar)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, Calendar value) throws RepositoryException {
        Value v = (value == null ? null : getValueFactory().createValue(value));
        return setProperty(name, v, PropertyType.DATE);
    }

    /**
     * @see Node#setProperty(String, Node)
     */
    @Override
    @CheckForNull
    public Property setProperty(String name, Node value) throws RepositoryException {
        Value v = (value == null) ? null : getValueFactory().createValue(value);
        return setProperty(name, v, PropertyType.REFERENCE);
    }

    @Override
    @Nonnull
    public Node getNode(final String relPath) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeImpl>() {
            @Override
            public NodeImpl perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathOrThrowNotFound(relPath);

                NodeDelegate nd = dlg.getChild(oakPath);
                if (nd == null) {
                    throw new PathNotFoundException(relPath);
                } else {
                    return new NodeImpl(nd);
                }
            }
        });
    }

    @Override
    @Nonnull
    public NodeIterator getNodes() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeIterator>() {
            @Override
            public NodeIterator perform() throws RepositoryException {
                Iterator<NodeDelegate> children = dlg.getChildren();
                long size = dlg.getChildCount();
                return new NodeIteratorAdapter(nodeIterator(children), size);
            }
        });
    }

    @Override
    @Nonnull
    public NodeIterator getNodes(final String namePattern)
            throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeIterator>() {
            @Override
            public NodeIterator perform() throws RepositoryException {
                Iterator<NodeDelegate> children = Iterators.filter(dlg.getChildren(),
                        new Predicate<NodeDelegate>() {
                            @Override
                            public boolean apply(NodeDelegate state) {
                                try {
                                    return ItemNameMatcher.matches(toJcrPath(state.getName()), namePattern);
                                } catch (InvalidItemStateException e) {
                                    return false;
                                }
                            }
                        });

                return new NodeIteratorAdapter(nodeIterator(children));
            }
        });
    }

    @Override
    @Nonnull
    public NodeIterator getNodes(final String[] nameGlobs) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeIterator>() {
            @Override
            public NodeIterator perform() throws RepositoryException {
                Iterator<NodeDelegate> children = Iterators.filter(dlg.getChildren(),
                        new Predicate<NodeDelegate>() {
                            @Override
                            public boolean apply(NodeDelegate state) {
                                try {
                                    return ItemNameMatcher.matches(toJcrPath(state.getName()), nameGlobs);
                                } catch (InvalidItemStateException e) {
                                    return false;
                                }
                            }
                        });

                return new NodeIteratorAdapter(nodeIterator(children));
            }
        });
    }

    @Override
    @Nonnull
    public Property getProperty(final String relPath) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyImpl>() {
            @Override
            public PropertyImpl perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathOrThrowNotFound(relPath);
                PropertyDelegate pd = dlg.getProperty(oakPath);
                if (pd == null) {
                    throw new PathNotFoundException(relPath + " not found on " + getPath());
                } else {
                    return new PropertyImpl(pd);
                }
            }
        });
    }

    @Override
    @Nonnull
    public PropertyIterator getProperties() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws RepositoryException {
                Iterator<PropertyDelegate> properties = dlg.getProperties();
                long size = dlg.getPropertyCount();
                return new PropertyIteratorAdapter(propertyIterator(properties), size);
            }
        });
    }

    @Override
    @Nonnull
    public PropertyIterator getProperties(final String namePattern) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws RepositoryException {
                Iterator<PropertyDelegate> properties = Iterators.filter(dlg.getProperties(),
                        new Predicate<PropertyDelegate>() {
                            @Override
                            public boolean apply(PropertyDelegate entry) {
                                try {
                                    return ItemNameMatcher.matches(toJcrPath(entry.getName()), namePattern);
                                } catch (InvalidItemStateException e) {
                                    return false;
                                }
                            }
                        });

                return new PropertyIteratorAdapter(propertyIterator(properties));
            }
        });
    }

    @Override
    @Nonnull
    public PropertyIterator getProperties(final String[] nameGlobs) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws RepositoryException {
                Iterator<PropertyDelegate> propertyNames = Iterators.filter(dlg.getProperties(),
                        new Predicate<PropertyDelegate>() {
                            @Override
                            public boolean apply(PropertyDelegate entry) {
                                try {
                                    return ItemNameMatcher.matches(toJcrPath(entry.getName()), nameGlobs);
                                } catch (InvalidItemStateException e) {
                                    return false;
                                }
                            }
                        });

                return new PropertyIteratorAdapter(propertyIterator(propertyNames));
            }
        });
    }

    /**
     * @see javax.jcr.Node#getPrimaryItem()
     */
    @Override
    @Nonnull
    public Item getPrimaryItem() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Item>() {
            @Override
            public Item perform() throws RepositoryException {
                String name = getPrimaryNodeType().getPrimaryItemName();
                if (name == null) {
                    throw new ItemNotFoundException("No primary item present on node " + this);
                }
                if (hasProperty(name)) {
                    return getProperty(name);
                } else if (hasNode(name)) {
                    return getNode(name);
                } else {
                    throw new ItemNotFoundException("Primary item " + name + " does not exist on node " + this);
                }
            }
        });
    }

    /**
     * @see javax.jcr.Node#getUUID()
     */
    @Override
    @Nonnull
    public String getUUID() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                if (isNodeType(NodeType.MIX_REFERENCEABLE)) {
                    return getIdentifier();
                }

                throw new UnsupportedRepositoryOperationException("Node is not referenceable.");
            }
        });
    }

    @Override
    @Nonnull
    public String getIdentifier() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return dlg.getIdentifier();
            }
        });
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
    @Nonnull
    public PropertyIterator getReferences() throws RepositoryException {
        return getReferences(null);
    }

    @Override
    @Nonnull
    public PropertyIterator getReferences(final String name) throws RepositoryException {
        checkStatus();
        return internalGetReferences(name, false);
    }

    /**
     * @see javax.jcr.Node#getWeakReferences()
     */
    @Override
    @Nonnull
    public PropertyIterator getWeakReferences() throws RepositoryException {
        return getWeakReferences(null);
    }

    @Override
    @Nonnull
    public PropertyIterator getWeakReferences(String name) throws RepositoryException {
        checkStatus();
        return internalGetReferences(name, true);
    }

    private PropertyIterator internalGetReferences(final String name, final boolean weak) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws InvalidItemStateException {
                IdentifierManager idManager = sessionDelegate.getIdManager();

                Set<String> propertyOakPaths = weak
                    ? idManager.getWeakReferences(dlg.getTree(), name)
                    : idManager.getReferences(dlg.getTree(), name);

                Iterable<Property> properties = Iterables.transform(
                    propertyOakPaths,
                    new Function<String, Property>() {
                        @Override
                        public Property apply(String oakPath) {
                            PropertyDelegate pd = sessionDelegate.getProperty(oakPath);
                            return pd == null ? null : new PropertyImpl(pd);
                        }
                    }
                );

                return new PropertyIteratorAdapter(properties.iterator(), propertyOakPaths.size());
            }
        });
    }

    @Override
    public boolean hasNode(final String relPath) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathOrThrow(relPath);
                return dlg.getChild(oakPath) != null;
            }
        });
    }

    @Override
    public boolean hasProperty(final String relPath) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = sessionDelegate.getOakPathOrThrow(relPath);
                return dlg.getProperty(oakPath) != null;
            }
        });
    }

    @Override
    public boolean hasNodes() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return dlg.getChildCount() != 0;
            }
        });
    }

    @Override
    public boolean hasProperties() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return dlg.getPropertyCount() != 0;
            }
        });
    }

    /**
     * @see javax.jcr.Node#getPrimaryNodeType()
     */
    @Override
    @Nonnull
    public NodeType getPrimaryNodeType() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeType>() {
            @Override
            public NodeType perform() throws RepositoryException {
                // TODO: check if transient changes to mixin-types are reflected here
                NodeTypeManager ntMgr = sessionDelegate.getNodeTypeManager();
                String primaryNtName;
                primaryNtName = hasProperty(Property.JCR_PRIMARY_TYPE)
                        ? getProperty(Property.JCR_PRIMARY_TYPE).getString()
                        : NodeType.NT_UNSTRUCTURED;

                return ntMgr.getNodeType(primaryNtName);
            }
        });
    }

    /**
     * @see javax.jcr.Node#getMixinNodeTypes()
     */
    @Override
    @Nonnull
    public NodeType[] getMixinNodeTypes() throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<NodeType[]>() {
            @Override
            public NodeType[] perform() throws RepositoryException {
                // TODO: check if transient changes to mixin-types are reflected here
                if (hasProperty(Property.JCR_MIXIN_TYPES)) {
                    NodeTypeManager ntMgr = sessionDelegate.getNodeTypeManager();
                    Value[] mixinNames = getProperty(Property.JCR_MIXIN_TYPES).getValues();
                    NodeType[] mixinTypes = new NodeType[mixinNames.length];
                    for (int i = 0; i < mixinNames.length; i++) {
                        mixinTypes[i] = ntMgr.getNodeType(mixinNames[i].getString());
                    }
                    return mixinTypes;
                } else {
                    return new NodeType[0];
                }
            }
        });
    }

    @Override
    public boolean isNodeType(final String nodeTypeName) throws RepositoryException {
        checkStatus();

        // TODO: figure out the right place for this check
        NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
        NodeType ntToCheck = ntm.getNodeType(nodeTypeName); // throws on not found
        String nameToCheck = ntToCheck.getName();

        NodeType currentPrimaryType = getPrimaryNodeType();
        if (currentPrimaryType.isNodeType(nameToCheck)) {
            return true;
        }

        for (NodeType mixin : getMixinNodeTypes()) {
            if (mixin.isNodeType(nameToCheck)) {
                return true;
            }
        }
        // TODO: END

        return false;
    }

    @Override
    public void setPrimaryType(final String nodeTypeName) throws RepositoryException {
        checkStatus();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                // TODO: figure out the right place for this check
                NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
                NodeType nt = ntm.getNodeType(nodeTypeName); // throws on not found
                if (nt.isAbstract() || nt.isMixin()) {
                    throw new ConstraintViolationException();
                }
                // TODO: END

                String jcrPrimaryType = sessionDelegate.getOakPathOrThrow(Property.JCR_PRIMARY_TYPE);
                CoreValue cv = ValueConverter.toCoreValue(nodeTypeName, PropertyType.NAME, sessionDelegate);
                dlg.setProperty(jcrPrimaryType, cv);
                return null;
            }
        });
    }

    @Override
    public void addMixin(final String mixinName) throws RepositoryException {
        checkStatus();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                // TODO: figure out the right place for this check
                NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
                NodeType nt = ntm.getNodeType(mixinName); // throws on not found
                // TODO: END

                String jcrMixinTypes = sessionDelegate.getOakPathOrThrow(Property.JCR_MIXIN_TYPES);
                PropertyDelegate mixins = dlg.getProperty(jcrMixinTypes);

                CoreValue cv = ValueConverter.toCoreValue(mixinName, PropertyType.NAME, sessionDelegate);

                boolean nodeModified = false;

                if (mixins == null) {
                    nodeModified = true;
                    dlg.setProperty(jcrMixinTypes, Collections.singletonList(cv));
                } else {
                    List<CoreValue> values = Lists.newArrayList(mixins.getValues());
                    if (!values.contains(cv)) {
                        values.add(cv);
                        nodeModified = true;
                        dlg.setProperty(jcrMixinTypes, values);
                    }
                }

                // TODO: hack -- make sure we assign a UUID
                if (nodeModified && nt.isNodeType(NodeType.MIX_REFERENCEABLE)) {
                    String jcrUuid = sessionDelegate.getOakPathOrThrow(Property.JCR_UUID);
                    String uuid = IdentifierManager.generateUUID();
                    dlg.setProperty(jcrUuid, ValueConverter.toCoreValue(uuid, PropertyType.STRING, sessionDelegate));
                }
                return null;
            }
        });
    }

    @Override
    public void removeMixin(final String mixinName) throws RepositoryException {
        checkStatus();

        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                if (!isNodeType(mixinName)) {
                    throw new NoSuchNodeTypeException();
                }

                throw new ConstraintViolationException();
            }
        });
    }

    @Override
    public boolean canAddMixin(final String mixinName) throws RepositoryException {
        checkStatus();

        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                // TODO: figure out the right place for this check
                NodeTypeManager ntm = sessionDelegate.getNodeTypeManager();
                ntm.getNodeType(mixinName); // throws on not found
                // TODO: END

                return isSupportedMixinName(mixinName);
            }
        });
    }

    @Override
    @Nonnull
    public NodeDefinition getDefinition() throws RepositoryException {
        if (getDepth() == 0) {
            // TODO: What should be the root node definition?
            return getPrimaryNodeType().getChildNodeDefinitions()[0];
        }

        String name = getName();
        // TODO: This may need to be optimized
        for (NodeType nt : getAllNodeTypes(getParent())) {
            for (NodeDefinition def : nt.getDeclaredChildNodeDefinitions()) {
                String defName = def.getName();
                if (name.equals(defName) || "*".equals(defName)) {
                    boolean match = true;
                    for (String type : def.getRequiredPrimaryTypeNames()) {
                        if (!isNodeType(type)) {
                            match = false;
                        }
                    }
                    if (match) {
                        return def;
                    }
                }
            }
        }

        throw new RepositoryException(
                "No matching node definition found for " + this);
    }

    @Override
    @Nonnull
    public String getCorrespondingNodePath(String workspaceName) throws RepositoryException {
        checkStatus();
        checkValidWorkspace(workspaceName);
        throw new UnsupportedRepositoryOperationException("TODO: Node.getCorrespondingNodePath");
    }


    @Override
    public void update(String srcWorkspace) throws RepositoryException {
        checkStatus();
        checkValidWorkspace(srcWorkspace);
        ensureNoPendingSessionChanges();

        // TODO
    }

    /**
     * @see javax.jcr.Node#checkin()
     */
    @Override
    @Nonnull
    public Version checkin() throws RepositoryException {
        return sessionDelegate.getVersionManager().checkin(getPath());
    }

    /**
     * @see javax.jcr.Node#checkout()
     */
    @Override
    public void checkout() throws RepositoryException {
        sessionDelegate.getVersionManager().checkout(getPath());
    }

    /**
     * @see javax.jcr.Node#doneMerge(javax.jcr.version.Version)
     */
    @Override
    public void doneMerge(Version version) throws RepositoryException {
        sessionDelegate.getVersionManager().doneMerge(getPath(), version);
    }

    /**
     * @see javax.jcr.Node#cancelMerge(javax.jcr.version.Version)
     */
    @Override
    public void cancelMerge(Version version) throws RepositoryException {
        sessionDelegate.getVersionManager().cancelMerge(getPath(), version);
    }

    /**
     * @see javax.jcr.Node#merge(String, boolean)
     */
    @Override
    @Nonnull
    public NodeIterator merge(String srcWorkspace, boolean bestEffort) throws RepositoryException {
        return sessionDelegate.getVersionManager().merge(getPath(), srcWorkspace, bestEffort);
    }

    /**
     * @see javax.jcr.Node#isCheckedOut()
     */
    @Override
    public boolean isCheckedOut() throws RepositoryException {
        try {
            return sessionDelegate.getVersionManager().isCheckedOut(getPath());
        } catch (UnsupportedRepositoryOperationException ex) {
            // when versioning is not supported all nodes are considered to be
            // checked out
            return true;
        }
    }

    /**
     * @see javax.jcr.Node#restore(String, boolean)
     */
    @Override
    public void restore(String versionName, boolean removeExisting) throws RepositoryException {
        sessionDelegate.getVersionManager().restore(getPath(), versionName, removeExisting);
    }

    /**
     * @see javax.jcr.Node#restore(javax.jcr.version.Version, boolean)
     */
    @Override
    public void restore(Version version, boolean removeExisting) throws RepositoryException {
        sessionDelegate.getVersionManager().restore(version, removeExisting);
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
        sessionDelegate.getVersionManager().restoreByLabel(getPath(), versionLabel, removeExisting);
    }

    /**
     * @see javax.jcr.Node#getVersionHistory()
     */
    @Override
    @Nonnull
    public VersionHistory getVersionHistory() throws RepositoryException {
        return sessionDelegate.getVersionManager().getVersionHistory(getPath());
    }

    /**
     * @see javax.jcr.Node#getBaseVersion()
     */
    @Override
    @Nonnull
    public Version getBaseVersion() throws RepositoryException {
        return sessionDelegate.getVersionManager().getBaseVersion(getPath());
    }

    /**
     * Checks whether this node is locked by looking for the
     * {@code jcr:lockOwner} property either on this node or
     * on any ancestor that also has the {@code jcr:lockIsDeep}
     * property set to {@code true}.
     */
    @Override
    public boolean isLocked() throws RepositoryException {
        String lockOwner = sessionDelegate.getOakPathOrThrow(JCR_LOCK_OWNER);
        String lockIsDeep = sessionDelegate.getOakPathOrThrow(JCR_LOCK_IS_DEEP);

        if (dlg.getProperty(lockOwner) != null) {
            return true;
        }

        NodeDelegate parent = dlg.getParent();
        while (parent != null) {
            if (parent.getProperty(lockOwner) != null) {
                PropertyDelegate isDeep = parent.getProperty(lockIsDeep);
                if (isDeep != null && !isDeep.isMultivalue()
                        && isDeep.getValue().getBoolean()) {
                    return true;
                }
            }
            parent = parent.getParent();
        }

        return false;
    }

    /**
     * Checks whether this node holds a lock by looking for the
     * {@code jcr:lockOwner} property.
     */
    @Override
    public boolean holdsLock() throws RepositoryException {
        String lockOwner = sessionDelegate.getOakPathOrThrow(JCR_LOCK_OWNER);
        return dlg.getProperty(lockOwner) != null;
    }

    /**
     * @see javax.jcr.Node#getLock()
     */
    @Override
    @Nonnull
    public Lock getLock() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * @see javax.jcr.Node#lock(boolean, boolean)
     */
    @Override
    @Nonnull
    public Lock lock(final boolean isDeep, boolean isSessionScoped)
            throws RepositoryException {
        final String userID = getSession().getUserID();

        String lockOwner = sessionDelegate.getOakPathOrThrow(JCR_LOCK_OWNER);
        String lockIsDeep = sessionDelegate.getOakPathOrThrow(JCR_LOCK_IS_DEEP);
        try {
            ContentSession session = sessionDelegate.getContentSession();
            CoreValueFactory factory = session.getCoreValueFactory();
            Root root = session.getCurrentRoot();
            Tree tree = root.getTree(dlg.getPath());
            if (tree == null) {
                throw new ItemNotFoundException();
            }
            tree.setProperty(lockOwner, factory.createValue(userID));
            tree.setProperty(lockIsDeep, factory.createValue(isDeep));
            root.commit(DefaultConflictHandler.OURS); // TODO: fail instead?
        } catch (CommitFailedException e) {
            throw new RepositoryException("Unable to lock " + this, e);
        }

        getSession().refresh(true);

        if (isSessionScoped) {
            return TODO.dummyImplementation().returnValue(new Lock() {
                @Override
                public String getLockOwner() {
                    return userID;
                }

                @Override
                public boolean isDeep() {
                    return isDeep;
                }

                @Override
                public Node getNode() {
                    return NodeImpl.this;
                }

                @Override
                public String getLockToken() {
                    return null;
                }

                @Override
                public long getSecondsRemaining() {
                    return Long.MAX_VALUE;
                }

                @Override
                public boolean isLive() {
                    return true;
                }

                @Override
                public boolean isSessionScoped() {
                    return true;
                }

                @Override
                public boolean isLockOwningSession() {
                    return true;
                }

                @Override
                public void refresh() {
                }
            });
        }

        return getLock();
    }

    /**
     * @see javax.jcr.Node#unlock()
     */
    @Override
    public void unlock() throws RepositoryException {
        String lockOwner = sessionDelegate.getOakPathOrThrow(JCR_LOCK_OWNER);
        String lockIsDeep = sessionDelegate.getOakPathOrThrow(JCR_LOCK_IS_DEEP);
        try {
            Root root = sessionDelegate.getContentSession().getCurrentRoot();
            Tree tree = root.getTree(dlg.getPath());
            if (tree == null) {
                throw new ItemNotFoundException();
            }
            tree.removeProperty(lockOwner);
            tree.removeProperty(lockIsDeep);
            root.commit(DefaultConflictHandler.OURS);
        } catch (CommitFailedException e) {
            throw new RepositoryException("Unable to unlock " + this, e);
        }

        getSession().refresh(true);
    }

    @Override
    @Nonnull
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
    @Nonnull
    public String[] getAllowedLifecycleTransistions() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Lifecycle Management is not supported");

    }

    //------------------------------------------------------------< private >---

    private static Iterator<Node> nodeIterator(Iterator<NodeDelegate> childNodes) {
        return Iterators.transform(
                childNodes,
                new Function<NodeDelegate, Node>() {
                    @Override
                    public Node apply(NodeDelegate nodeDelegate) {
                        return new NodeImpl(nodeDelegate);
                    }
                });
    }

    private static Iterator<Property> propertyIterator(Iterator<PropertyDelegate> properties) {
        return Iterators.transform(
                properties,
                new Function<PropertyDelegate, Property>() {
                    @Override
                    public Property apply(PropertyDelegate propertyDelegate) {
                        return new PropertyImpl(propertyDelegate);
                    }
                });
    }

    private static int getTargetType(Value value, int type) {
        if (value == null) {
            return PropertyType.STRING; // TODO: review again. rather use
            // property definition
        } else {
            return value.getType();
        }
    }

    private static int getTargetType(Value[] values, int type) {
        if (values == null || values.length == 0) {
            return PropertyType.STRING; // TODO: review again. rather use property definition
        } else {
            // TODO deal with values array containing a null value in the first position
            return getTargetType(values[0], type);
        }
    }

    // TODO: hack to filter for a subset of supported mixins for now
    // this allows exactly one (harmless) mixin type so that other code like
    // addMixin gets test coverage
    private boolean isSupportedMixinName(String mixinName) throws RepositoryException {
        String oakName = sessionDelegate.getOakPathOrThrow(mixinName);
        return "mix:title".equals(oakName);
    }

    private void checkValidWorkspace(String workspaceName) throws RepositoryException {
        for (String wn : sessionDelegate.getWorkspace().getAccessibleWorkspaceNames()) {
            if (wn.equals(workspaceName)) {
                return;
            }
        }
        throw new NoSuchWorkspaceException(workspaceName + " does not exist.");
    }
}
