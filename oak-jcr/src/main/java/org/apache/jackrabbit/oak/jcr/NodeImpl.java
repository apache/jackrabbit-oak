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

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.jcr.Property.JCR_LOCK_IS_DEEP;
import static javax.jcr.Property.JCR_LOCK_OWNER;
import static javax.jcr.PropertyType.UNDEFINED;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.lock.Lock;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.ItemNameMatcher;
import org.apache.jackrabbit.commons.iterator.NodeIteratorAdapter;
import org.apache.jackrabbit.commons.iterator.PropertyIteratorAdapter;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeType;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.util.TODO;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 *
 * @param <T> the delegate type
 */
public class NodeImpl<T extends NodeDelegate> extends ItemImpl<T> implements Node {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NodeImpl.class);

    public NodeImpl(T dlg, SessionContext sessionContext) {
        super(dlg, sessionContext);
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
        return perform(new ItemReadOperation<NodeImpl<NodeDelegate>>() {
            @Override
            public NodeImpl<NodeDelegate> perform() throws RepositoryException {
                if (dlg.isRoot()) {
                    throw new ItemNotFoundException("Root has no parent");
                } else {
                    NodeDelegate parent = dlg.getParent();
                    if (parent == null) {
                        throw new AccessDeniedException();
                    }
                    return new NodeImpl<NodeDelegate>(parent, sessionContext);
                }
            }
        });
    }

    /**
     * @see javax.jcr.Item#isNew()
     */
    @Override
    public boolean isNew() {
        return safePerform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() {
                return !dlg.isStale() && dlg.getStatus() == Status.NEW;
            }
        });
    }

    /**
     * @see javax.jcr.Item#isModified()
     */
    @Override
    public boolean isModified() {
        return safePerform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() {
                return !dlg.isStale() && dlg.getStatus() == Status.MODIFIED;
            }
        });
    }

    /**
     * @see javax.jcr.Item#remove()
     */
    @Override
    public void remove() throws RepositoryException {
        perform(new ItemWriteOperation<Void>() {
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
        checkAlive();
        visitor.visit(this);
    }

    //---------------------------------------------------------------< Node >---

    /**
     * @see Node#addNode(String)
     */
    @Override
    @Nonnull
    public Node addNode(String relPath) throws RepositoryException {
        return addNode(relPath, null);
    }

    @Override
    @Nonnull
    public Node addNode(final String relPath, final String primaryNodeTypeName) throws RepositoryException {
        return perform(new ItemWriteOperation<Node>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                super.checkPreconditions();
                SessionImpl.checkIndexOnName(sessionContext, relPath);
            }

            @Override
            public Node perform() throws RepositoryException {
                String oakPath = sessionContext.getOakPathOrThrowNotFound(relPath);
                String oakName = PathUtils.getName(oakPath);
                String parentPath = PathUtils.getParentPath(oakPath);

                NodeDelegate parent = dlg.getChild(parentPath);
                if (parent == null) {
                    // is it a property?
                    String grandParentPath = PathUtils.getParentPath(parentPath);
                    NodeDelegate grandParent = dlg.getChild(grandParentPath);
                    if (grandParent != null) {
                        String propName = PathUtils.getName(parentPath);
                        if (grandParent.getPropertyOrNull(propName) != null) {
                            throw new ConstraintViolationException("Can't add new node to property.");
                        }
                    }

                    throw new PathNotFoundException(relPath);
                }

                if (parent.getChild(oakName) != null) {
                    throw new ItemExistsException(relPath);
                }

                String ntName = primaryNodeTypeName;
                if (ntName == null) {
                    DefinitionProvider dp = getDefinitionProvider();
                    String childName = getOakName(PathUtils.getName(relPath));
                    NodeDefinition def = dp.getDefinition(parent.getTree(), childName);
                    ntName = def.getDefaultPrimaryTypeName();
                    if (ntName == null) {
                        throw new ConstraintViolationException(
                                "no matching child node definition found for " + relPath);
                    }
                }

                // TODO: figure out the right place for this check
                NodeType nt = getNodeTypeManager().getNodeType(ntName); // throws on not found
                if (nt.isAbstract() || nt.isMixin()) {
                    throw new ConstraintViolationException();
                }
                // TODO: END

                NodeDelegate added = parent.addChild(oakName);
                if (added == null) {
                    throw new ItemExistsException();
                }

                if (getPrimaryNodeType().hasOrderableChildNodes()) {
                    dlg.setOrderableChildren(true);
                }

                NodeImpl<?> childNode = new NodeImpl<NodeDelegate>(added, sessionContext);
                childNode.internalSetPrimaryType(ntName);
                childNode.autoCreateItems();
                return childNode;
            }
        });
    }

    @Override
    public void orderBefore(final String srcChildRelPath, final String destChildRelPath) throws RepositoryException {
        perform(new ItemWriteOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                getEffectiveNodeType().checkOrderableChildNodes();
                String oakSrcChildRelPath = getOakPathOrThrowNotFound(srcChildRelPath);
                String oakDestChildRelPath = null;
                if (destChildRelPath != null) {
                    oakDestChildRelPath = getOakPathOrThrowNotFound(destChildRelPath);
                }
                dlg.orderBefore(oakSrcChildRelPath, oakDestChildRelPath);
                return null;
            }
        });
    }

    //-------------------------------------------------------< setProperty >--
    //
    // The setProperty() variants below follow the same pattern:
    //
    //     if (value != null) {
    //         return internalSetProperty(name, ...);
    //     } else {
    //         return internalRemoveProperty(name);
    //     }
    //
    // In addition the value and value type information is pre-processed
    // according to the method signature before being passed to
    // internalSetProperty(). The methods that take a non-nullable
    // primitive value as an argument can skip the if clause.
    //
    // Note that due to backwards compatibility reasons (OAK-395) none
    // of the methods will ever return null, even if asked to remove
    // a non-existing property! See internalRemoveProperty() for details.

    @Override @Nonnull
    public Property setProperty(String name, Value value)
            throws RepositoryException {
        if (value != null) {
            return internalSetProperty(name, value, false);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, Value value, int type)
            throws RepositoryException {
        if (type == UNDEFINED) {
            return setProperty(name, value);
        } else if (value != null) {
            if (value.getType() != type) {
                value = ValueHelper.convert(value, type, getValueFactory());
            }
            return internalSetProperty(name, value, true);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, Value[] values)
            throws RepositoryException {
        if (values != null) {
            return internalSetProperty(name, values, ValueHelper.getType(values), false);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String jcrName, Value[] values, int type)
            throws RepositoryException {
        if (type == UNDEFINED) {
            return setProperty(jcrName, values);
        } else if (values != null) {
            values = ValueHelper.convert(values, type, getValueFactory());
            return internalSetProperty(jcrName, values, type, true);
        } else {
            return internalRemoveProperty(jcrName);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, String[] values)
            throws RepositoryException {
        if (values != null) {
            Value[] vs = ValueHelper.convert(
                    values, PropertyType.STRING, getValueFactory());
            return internalSetProperty(name, vs, UNDEFINED, false);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, String[] values, int type)
            throws RepositoryException {
        if (type == UNDEFINED) {
            return setProperty(name, values);
        } else if (values != null) {
            Value[] vs = ValueHelper.convert(values, type, getValueFactory());
            return internalSetProperty(name, vs, type, true);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, String value)
            throws RepositoryException {
        if (value != null) {
            Value v = getValueFactory().createValue(value);
            return internalSetProperty(name, v, false);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, String value, int type)
            throws RepositoryException {
        if (type == PropertyType.UNDEFINED) {
            return setProperty(name, value);
        } else if (value != null) {
            Value v = getValueFactory().createValue(value, type);
            return internalSetProperty(name, v, true);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, InputStream value)
            throws RepositoryException {
        if (value != null) {
            ValueFactory factory = getValueFactory();
            Binary binary = factory.createBinary(value);
            try {
                Value v = factory.createValue(binary);
                return internalSetProperty(name, v, false);
            } finally {
                binary.dispose();
            }
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, Binary value)
            throws RepositoryException {
        if (value != null) {
            Value v = getValueFactory().createValue(value);
            return internalSetProperty(name, v, false);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, boolean value)
            throws RepositoryException {
        Value v = getValueFactory().createValue(value);
        return internalSetProperty(name, v, false);
    }

    @Override @Nonnull
    public Property setProperty(String name, double value)
            throws RepositoryException {
        Value v = getValueFactory().createValue(value);
        return internalSetProperty(name, v, false);
    }

    @Override @Nonnull
    public Property setProperty(String name, BigDecimal value)
            throws RepositoryException {
        if (value != null) {
            Value v = getValueFactory().createValue(value);
            return internalSetProperty(name, v, false);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, long value)
            throws RepositoryException {
        Value v = getValueFactory().createValue(value);
        return internalSetProperty(name, v, false);
    }

    @Override @Nonnull
    public Property setProperty(String name, Calendar value)
            throws RepositoryException {
        if (value != null) {
            Value v = getValueFactory().createValue(value);
            return internalSetProperty(name, v, false);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override @Nonnull
    public Property setProperty(String name, Node value)
            throws RepositoryException {
        if (value != null) {
            Value v = getValueFactory().createValue(value);
            return internalSetProperty(name, v, false);
        } else {
            return internalRemoveProperty(name);
        }
    }

    @Override
    @Nonnull
    public Node getNode(final String relPath) throws RepositoryException {
        return perform(new ItemReadOperation<NodeImpl<?>>() {
            @Override
            public NodeImpl<?> perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(relPath);

                NodeDelegate nd = dlg.getChild(oakPath);
                if (nd == null) {
                    throw new PathNotFoundException(relPath);
                } else {
                    return new NodeImpl<NodeDelegate>(nd, sessionContext);
                }
            }
        });
    }

    @Override
    @Nonnull
    public NodeIterator getNodes() throws RepositoryException {
        return perform(new ItemReadOperation<NodeIterator>() {
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

        return perform(new ItemReadOperation<NodeIterator>() {
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
        return perform(new ItemReadOperation<NodeIterator>() {
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
        return perform(new ItemReadOperation<PropertyImpl>() {
            @Override
            public PropertyImpl perform() throws RepositoryException {
                String oakPath = getOakPathOrThrowNotFound(relPath);
                PropertyDelegate pd = dlg.getPropertyOrNull(oakPath);
                if (pd == null) {
                    throw new PathNotFoundException(relPath + " not found on " + getPath());
                } else {
                    return new PropertyImpl(pd, sessionContext);
                }
            }
        });
    }

    @Override
    @Nonnull
    public PropertyIterator getProperties() throws RepositoryException {
        return perform(new ItemReadOperation<PropertyIterator>() {
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
        return perform(new ItemReadOperation<PropertyIterator>() {
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
        return perform(new ItemReadOperation<PropertyIterator>() {
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
        return perform(new ItemReadOperation<Item>() {
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
        return perform(new ItemReadOperation<String>() {
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
        return perform(new ItemReadOperation<String>() {
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

    private PropertyIterator internalGetReferences(final String name, final boolean weak) throws RepositoryException {
        return perform(new ItemReadOperation<PropertyIterator>() {
            @Override
            public PropertyIterator perform() throws InvalidItemStateException {
                IdentifierManager idManager = sessionDelegate.getIdManager();

                Set<String> propertyOakPaths = idManager.getReferences(weak, dlg.getTree(), name);
                Iterable<Property> properties = Iterables.transform(
                        propertyOakPaths,
                        new Function<String, Property>() {
                            @Override
                            public Property apply(String oakPath) {
                                PropertyDelegate pd = sessionDelegate.getProperty(oakPath);
                                return pd == null ? null : new PropertyImpl(pd, sessionContext);
                            }
                        }
                );

                return new PropertyIteratorAdapter(properties.iterator(), propertyOakPaths.size());
            }
        });
    }

    /**
     * @see javax.jcr.Node#getReferences()
     */
    @Override
    @Nonnull
    public PropertyIterator getReferences() throws RepositoryException {
        return internalGetReferences(null, false);
    }

    @Override
    @Nonnull
    public PropertyIterator getReferences(final String name) throws RepositoryException {
        return internalGetReferences(name, false);
    }

    /**
     * @see javax.jcr.Node#getWeakReferences()
     */
    @Override
    @Nonnull
    public PropertyIterator getWeakReferences() throws RepositoryException {
        return internalGetReferences(null, true);
    }

    @Override
    @Nonnull
    public PropertyIterator getWeakReferences(String name) throws RepositoryException {
        return internalGetReferences(name, true);
    }

    @Override
    public boolean hasNode(final String relPath) throws RepositoryException {
        return perform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = getOakPathOrThrow(relPath);
                return dlg.getChild(oakPath) != null;
            }
        });
    }

    @Override
    public boolean hasProperty(final String relPath) throws RepositoryException {
        return perform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                String oakPath = getOakPathOrThrow(relPath);
                return dlg.getPropertyOrNull(oakPath) != null;
            }
        });
    }

    @Override
    public boolean hasNodes() throws RepositoryException {
        return perform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return dlg.getChildCount() != 0;
            }
        });
    }

    @Override
    public boolean hasProperties() throws RepositoryException {
        return perform(new ItemReadOperation<Boolean>() {
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
        return perform(new ItemReadOperation<NodeType>() {
            @Override
            public NodeType perform() throws RepositoryException {
                String primaryNtName;
                if (hasProperty(Property.JCR_PRIMARY_TYPE)) {
                    primaryNtName = getProperty(Property.JCR_PRIMARY_TYPE).getString();
                } else {
                    throw new RepositoryException("Node " + getPath() + " doesn't have primary type set.");
                }
                return getNodeTypeManager().getNodeType(primaryNtName);
            }
        });
    }

    /**
     * @see javax.jcr.Node#getMixinNodeTypes()
     */
    @Override
    @Nonnull
    public NodeType[] getMixinNodeTypes() throws RepositoryException {
        return perform(new ItemReadOperation<NodeType[]>() {
            @Override
            public NodeType[] perform() throws RepositoryException {
                // TODO: check if transient changes to mixin-types are reflected here
                if (hasProperty(Property.JCR_MIXIN_TYPES)) {
                    NodeTypeManager ntMgr = getNodeTypeManager();
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
        return perform(new ItemReadOperation<Boolean>() {
            @Override
            protected Boolean perform() throws RepositoryException {
                String oakName = getOakName(nodeTypeName);
                return getEffectiveNodeTypeProvider().isNodeType(dlg.getTree(), oakName);
            }
        });
    }

    @Override
    public void setPrimaryType(final String nodeTypeName) throws RepositoryException {
        perform(new ItemWriteOperation<Void>() {
            @Override
            protected void checkPreconditions() throws RepositoryException {
                super.checkPreconditions();
                if (!isCheckedOut()) {
                    throw new VersionException("Cannot set primary type. Node is checked in.");
                }
            }

            @Override
            protected Void perform() throws RepositoryException {
                internalSetPrimaryType(nodeTypeName);
                return null;
            }
        });
    }

    @Override
    public void addMixin(final String mixinName) throws RepositoryException {
        perform(new ItemWriteOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                // TODO: figure out the right place for this check
                getNodeTypeManager().getNodeType(mixinName); // throws on not found
                // TODO: END

                if (isNodeType(mixinName)) {
                    return null;
                }

                PropertyDelegate mixins = dlg.getPropertyOrNull(JcrConstants.JCR_MIXINTYPES);
                Value value = getValueFactory().createValue(mixinName, PropertyType.NAME);

                boolean nodeModified = false;
                if (mixins == null) {
                    nodeModified = true;
                    dlg.setProperty(PropertyStates.createProperty(
                            JcrConstants.JCR_MIXINTYPES, Collections.singletonList(value)));
                } else {
                    PropertyState property = mixins.getMultiState();
                    List<Value> values = ValueFactoryImpl.createValues(property, sessionContext);
                    if (!values.contains(value)) {
                        values.add(value);
                        nodeModified = true;
                        dlg.setProperty(PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, values));
                    }
                }

                if (nodeModified) {
                    autoCreateItems();
                }
                return null;
            }
        });
    }

    @Override
    public void removeMixin(final String mixinName) throws RepositoryException {
        perform(new ItemWriteOperation<Void>() {
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
        return perform(new ItemReadOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                // TODO: figure out the right place for this check
                getNodeTypeManager().getNodeType(mixinName); // throws on not found
                // TODO: END

                return getEffectiveNodeType().supportsMixin(mixinName);
            }
        });
    }

    @Override
    @Nonnull
    public NodeDefinition getDefinition() throws RepositoryException {
        return perform(new ItemReadOperation<NodeDefinition>() {
            @Override
            protected NodeDefinition perform() throws RepositoryException {
                return internalGetDefinition();
            }
        });
    }

    @Override
    @Nonnull
    public String getCorrespondingNodePath(String workspaceName) throws RepositoryException {
        checkAlive();
        checkValidWorkspace(workspaceName);
        throw new UnsupportedRepositoryOperationException("TODO: Node.getCorrespondingNodePath");
    }


    @Override
    public void update(String srcWorkspace) throws RepositoryException {
        checkAlive();
        checkValidWorkspace(srcWorkspace);

        // check for pending changes
        if (sessionDelegate.hasPendingChanges()) {
            String msg = "Unable to perform operation. Session has pending changes.";
            log.debug(msg);
            throw new InvalidItemStateException(msg);
        }

        // TODO
    }

    /**
     * @see javax.jcr.Node#checkin()
     */
    @Override
    @Nonnull
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
    @Nonnull
    public NodeIterator merge(String srcWorkspace, boolean bestEffort) throws RepositoryException {
        return getVersionManager().merge(getPath(), srcWorkspace, bestEffort);
    }

    /**
     * @see javax.jcr.Node#isCheckedOut()
     */
    @Override
    public boolean isCheckedOut() throws RepositoryException {
        try {
            return getVersionManager().isCheckedOut(getPath());
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
    @Nonnull
    public VersionHistory getVersionHistory() throws RepositoryException {
        return getVersionManager().getVersionHistory(getPath());
    }

    /**
     * @see javax.jcr.Node#getBaseVersion()
     */
    @Override
    @Nonnull
    public Version getBaseVersion() throws RepositoryException {
        return getVersionManager().getBaseVersion(getPath());
    }

    /**
     * Checks whether this node is locked by looking for the
     * {@code jcr:lockOwner} property either on this node or
     * on any ancestor that also has the {@code jcr:lockIsDeep}
     * property set to {@code true}.
     */
    @Override
    public boolean isLocked() throws RepositoryException {
        return perform(new ItemReadOperation<Boolean>() {
            @Override
            protected Boolean perform() throws RepositoryException {
                String lockOwner = getOakPathOrThrow(JCR_LOCK_OWNER);
                String lockIsDeep = getOakPathOrThrow(JCR_LOCK_IS_DEEP);

                if (dlg.getPropertyOrNull(lockOwner) != null) {
                    return true;
                }

                NodeDelegate parent = dlg.getParent();
                while (parent != null) {
                    if (parent.getPropertyOrNull(lockOwner) != null) {
                        PropertyDelegate isDeep = parent.getPropertyOrNull(lockIsDeep);
                        if (isDeep != null && !isDeep.isArray()) {
                            if (isDeep.getBoolean()) {
                                return true;
                            }
                        }
                    }
                    parent = parent.getParent();
                }

                return false;
            }
        });
    }

    /**
     * Checks whether this node holds a lock by looking for the
     * {@code jcr:lockOwner} property.
     */
    @Override
    public boolean holdsLock() throws RepositoryException {
        return perform(new ItemReadOperation<Boolean>() {
            @Override
            protected Boolean perform() throws RepositoryException {
                String lockOwner = getOakPathOrThrow(JCR_LOCK_OWNER);
                return dlg.getPropertyOrNull(lockOwner) != null;
            }
        });
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
        ContentSession session = sessionDelegate.getContentSession();
        final String userID = session.getAuthInfo().getUserID();

        String lockOwner = getOakPathOrThrow(JCR_LOCK_OWNER);
        String lockIsDeep = getOakPathOrThrow(JCR_LOCK_IS_DEEP);
        try {
            Root root = session.getLatestRoot();
            Tree tree = root.getTree(dlg.getPath());
            if (tree == null) {
                throw new ItemNotFoundException();
            }
            tree.setProperty(lockOwner, userID);
            tree.setProperty(lockIsDeep, isDeep);
            root.commit(); // TODO: fail instead?
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
        String lockOwner = getOakPathOrThrow(JCR_LOCK_OWNER);
        String lockIsDeep = getOakPathOrThrow(JCR_LOCK_IS_DEEP);
        try {
            Root root = sessionDelegate.getContentSession().getLatestRoot();
            Tree tree = root.getTree(dlg.getPath());
            if (tree == null) {
                throw new ItemNotFoundException();
            }
            tree.removeProperty(lockOwner);
            tree.removeProperty(lockIsDeep);
            root.commit();
        } catch (CommitFailedException e) {
            throw new RepositoryException("Unable to unlock " + this, e);
        }

        getSession().refresh(true);
    }

    @Override
    @Nonnull
    public NodeIterator getSharedSet() throws RepositoryException {
        return perform(new ItemReadOperation<NodeIterator>() {
            @Override
            protected NodeIterator perform() {
                return new NodeIteratorAdapter(ImmutableSet.of(NodeImpl.this));
            }
        });
    }

    @Override
    public void removeSharedSet() throws RepositoryException {
        perform(new ItemWriteOperation<Void>() {
            @Override
            protected Void perform() throws RepositoryException {
                NodeIterator sharedSet = getSharedSet();
                while (sharedSet.hasNext()) {
                    sharedSet.nextNode().removeShare();
                }
                return null;
            }
        });
    }

    @Override
    public void removeShare() throws RepositoryException {
        remove();
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

    //------------------------------------------------------------< internal >---

    @Override
    protected final NodeDefinition internalGetDefinition() throws RepositoryException {
        NodeDelegate parent = dlg.getParent();
        if (parent == null) {
            return getDefinitionProvider().getRootDefinition();
        } else {
            return getDefinitionProvider().getDefinition(
                    parent.getTree(), dlg.getTree());
        }
    }

    private EffectiveNodeType getEffectiveNodeType() throws RepositoryException {
        return getEffectiveNodeTypeProvider().getEffectiveNodeType(dlg.getTree());
    }

    private Iterator<Node> nodeIterator(Iterator<NodeDelegate> childNodes) {
        return Iterators.transform(
                childNodes,
                new Function<NodeDelegate, Node>() {
                    @Override
                    public Node apply(NodeDelegate nodeDelegate) {
                        return new NodeImpl<NodeDelegate>(nodeDelegate, sessionContext);
                    }
                });
    }

    private Iterator<Property> propertyIterator(Iterator<PropertyDelegate> properties) {
        return Iterators.transform(
                properties,
                new Function<PropertyDelegate, Property>() {
                    @Override
                    public Property apply(PropertyDelegate propertyDelegate) {
                        return new PropertyImpl(propertyDelegate, sessionContext);
                    }
                });
    }

    private void autoCreateItems() throws RepositoryException {
        EffectiveNodeType effective = getEffectiveNodeType();
        for (PropertyDefinition pd : effective.getAutoCreatePropertyDefinitions()) {
            if (dlg.getPropertyOrNull(pd.getName()) == null) {
                if (pd.isMultiple()) {
                    dlg.setProperty(PropertyStates.createProperty(pd.getName(), getAutoCreatedValues(pd)));
                } else {
                    dlg.setProperty(PropertyStates.createProperty(pd.getName(), getAutoCreatedValue(pd)));
                }
            }
        }
        for (NodeDefinition nd : effective.getAutoCreateNodeDefinitions()) {
            if (dlg.getChild(nd.getName()) == null) {
                autoCreateNode(nd);
            }
        }
    }

    private Value getAutoCreatedValue(PropertyDefinition definition)
            throws RepositoryException {
        String name = definition.getName();
        String declaringNT = definition.getDeclaringNodeType().getName();

        if (NodeTypeConstants.JCR_UUID.equals(name)) {
            // jcr:uuid property of the mix:referenceable node type
            if (NodeTypeConstants.MIX_REFERENCEABLE.equals(declaringNT)) {
                return getValueFactory().createValue(IdentifierManager.generateUUID());
            }
        } else if (NodeTypeConstants.JCR_CREATED.equals(name)) {
            // jcr:created property of a version or a mix:created
            if (NodeTypeConstants.MIX_CREATED.equals(declaringNT)
                    || NodeTypeConstants.NT_VERSION.equals(declaringNT)) {
                return getValueFactory().createValue(Calendar.getInstance());
            }
        } else if (NodeTypeConstants.JCR_CREATEDBY.equals(name)) {
            String userID = sessionDelegate.getAuthInfo().getUserID();
            // jcr:createdBy property of a mix:created
            if (userID != null
                    && NodeTypeConstants.MIX_CREATED.equals(declaringNT)) {
                return getValueFactory().createValue(userID);
            }
        } else if (NodeTypeConstants.JCR_LASTMODIFIED.equals(name)) {
            // jcr:lastModified property of a mix:lastModified
            if (NodeTypeConstants.MIX_LASTMODIFIED.equals(declaringNT)) {
                return getValueFactory().createValue(Calendar.getInstance());
            }
        } else if (NodeTypeConstants.JCR_LASTMODIFIEDBY.equals(name)) {
            String userID = sessionDelegate.getAuthInfo().getUserID();
            // jcr:lastModifiedBy property of a mix:lastModified
            if (userID != null
                    && NodeTypeConstants.MIX_LASTMODIFIED.equals(declaringNT)) {
                return getValueFactory().createValue(userID);
            }
        }
        // does the definition have a default value?
        if (definition.getDefaultValues() != null) {
            Value[] values = definition.getDefaultValues();
            if (values.length > 0) {
                return values[0];
            }
        }
        throw new RepositoryException("Unable to auto-create value for " +
                PathUtils.concat(getPath(), name));
    }

    private Iterable<Value> getAutoCreatedValues(PropertyDefinition definition)
            throws RepositoryException {
        String name = definition.getName();

        // default values?
        if (definition.getDefaultValues() != null) {
            return Lists.newArrayList(definition.getDefaultValues());
        }
        throw new RepositoryException("Unable to auto-create value for " +
                PathUtils.concat(getPath(), name));
    }

    private void autoCreateNode(NodeDefinition definition)
            throws RepositoryException {
        addNode(definition.getName(), definition.getDefaultPrimaryTypeName());
    }

    private void checkValidWorkspace(String workspaceName) throws RepositoryException {
        Workspace workspace = sessionContext.getWorkspace();
        for (String wn : workspace.getAccessibleWorkspaceNames()) {
            if (wn.equals(workspaceName)) {
                return;
            }
        }
        throw new NoSuchWorkspaceException(workspaceName + " does not exist.");
    }

    private void internalSetPrimaryType(final String nodeTypeName) throws RepositoryException {
        // TODO: figure out the right place for this check
        NodeType nt = getNodeTypeManager().getNodeType(nodeTypeName); // throws on not found
        if (nt.isAbstract() || nt.isMixin()) {
            throw new ConstraintViolationException();
        }
        // TODO: END

        String jcrPrimaryType = getOakPathOrThrow(Property.JCR_PRIMARY_TYPE);
        Value value = getValueFactory().createValue(nodeTypeName, PropertyType.NAME);

        dlg.setProperty(PropertyStates.createProperty(jcrPrimaryType, value));
        dlg.setOrderableChildren(nt.hasOrderableChildNodes());
    }

    private Property internalSetProperty(
            String jcrName, final Value value, final boolean exactTypeMatch)
            throws RepositoryException {
        final String oakName = getOakPathOrThrow(checkNotNull(jcrName));
        checkNotNull(value);
        return perform(new ItemWriteOperation<Property>() {
            @Override
            public Property perform() throws RepositoryException {
                // TODO: Avoid extra JCR method calls (OAK-672)
                PropertyDefinition definition = getEffectiveNodeType()
                        .getPropertyDefinition(oakName, false, value.getType(), exactTypeMatch);

                checkProtected(definition);
                PropertyState state = createSingleState(oakName, value, definition);
                return new PropertyImpl(dlg.setProperty(state), sessionContext);
            }
        });
    }

    private Property internalSetProperty(
            String jcrName, final Value[] values,
            final int type, final boolean exactTypeMatch)
            throws RepositoryException {
        final String oakName = getOakPathOrThrow(checkNotNull(jcrName));
        checkNotNull(values);
        return perform(new ItemWriteOperation<Property>() {
            @Override
            public Property perform() throws RepositoryException {
                // TODO: Avoid extra JCR method calls (OAK-672)
                PropertyDefinition definition = getEffectiveNodeType()
                        .getPropertyDefinition(oakName, true, type, exactTypeMatch);

                checkProtected(definition);
                PropertyState state = createMultiState(oakName, type, values, definition);
                return new PropertyImpl(dlg.setProperty(state), sessionContext);
            }
        });
    }

    private Property internalRemoveProperty(final String jcrName)
            throws RepositoryException {
        final String oakName = getOakPathOrThrow(checkNotNull(jcrName));
        return perform(new ItemWriteOperation<Property>() {
            @Override
            protected Property perform() throws RepositoryException {
                // TODO: Avoid extra JCR method calls (OAK-672)
                if (hasProperty(jcrName)) {
                    Property property = getProperty(jcrName);
                    property.remove();
                    return property;
                } else {
                    // Return a property instance which throws on access. See OAK-395
                    return new PropertyImpl(dlg.getProperty(oakName), sessionContext);
                }
            }
        });
    }

}
