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

import java.util.Arrays;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.ItemDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionOperation;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.value.ValueHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
abstract class ItemImpl<T extends ItemDelegate> implements Item {
    private static final Logger log = LoggerFactory.getLogger(ItemImpl.class);

    protected final SessionContext sessionContext;
    protected final T dlg;
    protected final SessionDelegate sessionDelegate;

    protected ItemImpl(T itemDelegate, SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.dlg = itemDelegate;
        this.sessionDelegate = sessionContext.getSessionDelegate();
    }

    protected abstract class ItemReadOperation<U> extends SessionOperation<U> {
        @Override
        protected void checkPreconditions() throws RepositoryException {
            checkAlive();
        }
    }

    protected abstract class ItemWriteOperation<U> extends SessionOperation<U> {
        @Override
        protected void checkPreconditions() throws RepositoryException {
            checkAlive();
            checkProtected();
        }
    }

    /**
     * Perform the passed {@link org.apache.jackrabbit.oak.jcr.ItemImpl.ItemReadOperation}.
     * @param op  operation to perform
     * @param <U>  return type of the operation
     * @return  the result of {@code op.perform()}
     * @throws RepositoryException as thrown by {@code op.perform()}.
     */
    @CheckForNull
    protected final <U> U perform(@Nonnull SessionOperation<U> op) throws RepositoryException {
        return sessionDelegate.perform(op);
    }

    /**
     * Perform the passed {@link org.apache.jackrabbit.oak.jcr.ItemImpl.ItemReadOperation} assuming it does not throw an
     * {@code RepositoryException}. If it does, wrap it into and throw it as an
     * {@code IllegalArgumentException}.
     * @param op  operation to perform
     * @param <U>  return type of the operation
     * @return  the result of {@code op.perform()}
     */
    @CheckForNull
    protected final <U> U safePerform(@Nonnull SessionOperation<U> op) {
        try {
            return sessionDelegate.perform(op);
        }
        catch (RepositoryException e) {
            String msg = "Unexpected exception thrown by operation " + op;
            log.error(msg, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    //---------------------------------------------------------------< Item >---

    /**
     * @see javax.jcr.Item#getName()
     */
    @Override
    @Nonnull
    public String getName() throws RepositoryException {
        return perform(new ItemReadOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                String oakName = dlg.getName();
                // special case name of root node
                return oakName.isEmpty() ? "" : toJcrPath(dlg.getName());
            }
        });
    }

    /**
     * @see javax.jcr.Property#getPath()
     */
    @Override
    @Nonnull
    public String getPath() throws RepositoryException {
        return perform(new ItemReadOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return toJcrPath(dlg.getPath());
            }
        });
    }

    @Override
    @Nonnull
    public Session getSession() throws RepositoryException {
        return sessionContext.getSession();
    }

    @Override
    public Item getAncestor(final int depth) throws RepositoryException {
        return perform(new ItemReadOperation<Item>() {
            @Override
            protected Item perform() throws RepositoryException {
                if (depth < 0) {
                    throw new ItemNotFoundException(this + ": Invalid ancestor depth (" + depth + ')');
                } else if (depth == 0) {
                    NodeDelegate nd = sessionDelegate.getRootNode();
                    if (nd == null) {
                        throw new AccessDeniedException("Root node is not accessible.");
                    }
                    return sessionContext.createNodeOrNull(nd);
                }

                String path = dlg.getPath();
                int slash = 0;
                for (int i = 0; i < depth - 1; i++) {
                    slash = PathUtils.getNextSlash(path, slash + 1);
                    if (slash == -1) {
                        throw new ItemNotFoundException(this + ": Invalid ancestor depth (" + depth + ')');
                    }
                }
                slash = PathUtils.getNextSlash(path, slash + 1);
                if (slash == -1) {
                    return ItemImpl.this;
                }

                NodeDelegate nd = sessionDelegate.getNode(path.substring(0, slash));
                if (nd == null) {
                    throw new AccessDeniedException(this + ": Ancestor access denied (" + depth + ')');
                }
                return sessionContext.createNodeOrNull(nd);
            }
        });
    }

    @Override
    public int getDepth() throws RepositoryException {
        return perform(new ItemReadOperation<Integer>() {
            @Override
            public Integer perform() throws RepositoryException {
                return PathUtils.getDepth(dlg.getPath());
            }
        });
    }

    /**
     * @see Item#isSame(javax.jcr.Item)
     */
    @Override
    public boolean isSame(Item otherItem) throws RepositoryException {
        if (this == otherItem) {
            return true;
        }

        // The objects are either both Node objects or both Property objects.
        if (isNode() != otherItem.isNode()) {
            return false;
        }

        // Test if both items belong to the same repository
        // created by the same Repository object
        if (!getSession().getRepository().equals(otherItem.getSession().getRepository())) {
            return false;
        }

        // Both objects were acquired through Session objects bound to the same
        // repository workspace.
        if (!getSession().getWorkspace().getName().equals(otherItem.getSession().getWorkspace().getName())) {
            return false;
        }

        if (isNode()) {
            return ((Node) this).getIdentifier().equals(((Node) otherItem).getIdentifier());
        } else {
            return getName().equals(otherItem.getName()) && getParent().isSame(otherItem.getParent());
        }
    }

    /**
     * @see javax.jcr.Item#save()
     */
    @Override
    public void save() throws RepositoryException {
        log.warn("Item#save is no longer supported. Please use Session#save instead.");
        
        if (isNew()) {
            throw new RepositoryException("Item.save() not allowed on new item");
        }
        
        getSession().save();
    }

    /**
     * @see Item#refresh(boolean)
     */
    @Override
    public void refresh(boolean keepChanges) throws RepositoryException {
        log.warn("Item#refresh is no longer supported. Please use Session#refresh");
        getSession().refresh(keepChanges);
    }

    @Override
    public String toString() {
        return (isNode() ? "Node[" : "Property[") + dlg + ']';
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Performs a sanity check on this item and the associated session.
     *
     * @throws RepositoryException if this item has been rendered invalid for some reason
     * or the associated session has been logged out.
     */
    void checkAlive() throws RepositoryException {
        sessionDelegate.checkAlive();
        dlg.checkNotStale();
    }

    protected abstract ItemDefinition internalGetDefinition() throws RepositoryException;

    void checkProtected() throws RepositoryException {
        if (dlg.isProtected()) {
            throw new ConstraintViolationException("Item is protected.");
        }
    }

    void checkProtected(ItemDefinition definition) throws ConstraintViolationException {
        if (definition.isProtected()) {
            throw new ConstraintViolationException("Item is protected.");
        }
    }

    @Nonnull
    String getOakName(String name) throws RepositoryException {
        return sessionContext.getOakName(name);
    }

    @Nonnull
    String getOakPathOrThrow(String jcrPath) throws RepositoryException {
        return sessionContext.getOakPathOrThrow(jcrPath);
    }

    @Nonnull
    String getOakPathOrThrowNotFound(String relPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(relPath);
    }

    @Nonnull
    String toJcrPath(String oakPath) {
        return sessionContext.getJcrPath(oakPath);
    }

    /**
     * Returns the value factory associated with the editing session.
     *
     * @return the value factory
     */
    @Nonnull
    ValueFactory getValueFactory() {
        return sessionContext.getValueFactory();
    }

    @Nonnull
    NodeTypeManager getNodeTypeManager() {
        return sessionContext.getNodeTypeManager();
    }

    @Nonnull
    DefinitionProvider getDefinitionProvider() {
        return sessionContext.getDefinitionProvider();
    }

    @Nonnull
    EffectiveNodeTypeProvider getEffectiveNodeTypeProvider() {
        return sessionContext.getEffectiveNodeTypeProvider();
    }

    @Nonnull
    VersionManager getVersionManager() throws RepositoryException {
        return sessionContext.getVersionManager();
    }

    /**
     * Determine property type from property definition and default to
     * {@code defaultType} in the case of {@link javax.jcr.PropertyType#UNDEFINED}.
     *
     * @param definition
     * @param defaultType
     * @return the type determined from the property definition
     */
    private static int getType(PropertyDefinition definition, int defaultType) {
        if (definition.getRequiredType() == PropertyType.UNDEFINED) {
            return defaultType == PropertyType.UNDEFINED ? PropertyType.STRING : defaultType;
        } else {
            return definition.getRequiredType();
        }
    }

    /**
     * Removes all {@code null} values from the given array.
     *
     * @param values value array
     * @return value array without {@code null} entries
     */
    private static Value[] compact(Value[] values) {
        int n = 0;
        for (Value value : values) {
            if (value != null) {
                n++;
            }
        }
        if (n == values.length) {
            return values;
        } else {
            Value[] nonNullValues = new Value[n];
            for (int i = 0, j = 0; i < values.length; i++) {
                if (values[i] != null) {
                    nonNullValues[j++] = values[i];
                }
            }
            return nonNullValues;
        }
    }

    /**
     * Create a single value property state given a {@code name}, a {@code value} and a
     * property {@code definition}. If the value does not match the property definition
     * it is converted accordingly
     *
     * @param name
     * @param value
     * @param definition
     * @return  single valued property state
     * @throws ValueFormatException  if {@code definition} defines a multi valued property
     * @throws RepositoryException  if value conversion fails
     */
    PropertyState createSingleState(String name, Value value, PropertyDefinition definition)
            throws RepositoryException {
        if (definition.isMultiple()) {
            throw new ValueFormatException("Cannot set single value to multivalued property");
        }

        int targetType = getType(definition, value.getType());
        if (targetType == value.getType()) {
            return PropertyStates.createProperty(name, value);
        }
        else {
            return PropertyStates.createProperty(name, ValueHelper.convert(
                    value, targetType, getValueFactory()));
        }
    }

    /**
     * Create a single value property state given a {@code name}, a {@code type}, a
     * {@code value} and a property {@code definition}. If the type does not match the
     * property definition it is converted accordingly
     *
     * @param name
     * @param type
     * @param values
     * @param definition
     * @return  array valued property state
     * @throws ValueFormatException  if {@code definition} does not define a multi valued property
     * @throws RepositoryException  if value conversion fails
     */
    PropertyState createMultiState(String name, int type, Value[] values, PropertyDefinition definition)
            throws RepositoryException {
        if (!definition.isMultiple()) {
            throw new ValueFormatException("Cannot set value array to single value property");
        }

        Value[] nonNullValues = compact(values);
        int targetType = getType(definition, type);
        if (nonNullValues.length == 0) {
            return MemoryPropertyBuilder
                    .array(Type.fromTag(type, false), name)
                    .getPropertyState();
        } else if (targetType == type) {
            return PropertyStates.createProperty(name, Arrays.asList(nonNullValues));
        } else {
            return PropertyStates.createProperty(name, Arrays.asList(ValueHelper.convert(
                    values, targetType, getValueFactory())));
        }
    }

}
