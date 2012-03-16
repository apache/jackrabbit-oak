/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.jcr.state;

import org.apache.jackrabbit.oak.jcr.SessionImpl.Context;
import org.apache.jackrabbit.oak.jcr.json.JsonValue;
import org.apache.jackrabbit.oak.jcr.json.JsonValue.JsonAtom;
import org.apache.jackrabbit.oak.jcr.state.ChangeTree.NodeDelta;
import org.apache.jackrabbit.oak.jcr.util.Function1;
import org.apache.jackrabbit.oak.jcr.util.Iterators;
import org.apache.jackrabbit.oak.jcr.util.PagedIterator;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.apache.jackrabbit.oak.jcr.util.Predicate;
import org.apache.jackrabbit.oak.model.ChildNodeEntry;
import org.apache.jackrabbit.oak.model.NodeState;
import org.apache.jackrabbit.oak.model.PropertyState;

import javax.jcr.ItemExistsException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.PathNotFoundException;
import java.util.Iterator;

import static org.apache.jackrabbit.oak.jcr.util.Iterators.toIterable;


/**
 * A {@code TransientNodeState} instance uses a {@code TransientSpace} to record changes
 * to a {@code PersistedNodeState}.
 */
public class TransientNodeState {
    private static final int BATCH_SIZE = 256;

    private final Context sessionContext;

    private String revision;
    private NodeState persistentNodeState;
    private NodeDelta nodeDelta;

    TransientNodeState(Context sessionContext, NodeDelta nodeDelta) {
        this.sessionContext = sessionContext;
        this.nodeDelta = nodeDelta;
    }

    /**
     * @return {@code true} iff this is the root node
     */
    public boolean isRoot() {
        return getPath().isRoot();
    }

    /**
     * @return the path of this node
     */
    public Path getPath() {
        return getNodeDelta().getPath();
    }

    /**
     * @return the name of this node
     */
    public String getName() {
        return getPath().getName();
    }

    /**
     * @return {@code true} iff this node has been transiently added.
     */
    public boolean isNew() {
        NodeDelta delta = getNodeDelta();
        return delta.isTransient() && !delta.isRemoved();
    }

    /**
     * @return {@code true} iff this node has been transiently modified.
     */
    public boolean isModified() {
        return getNodeDelta().isTransient();
    }

    /**
     * Transiently add a node with the given {@code name}.
     *
     * @param name The name of the new node.
     * @return the added node
     * @throws javax.jcr.ItemExistsException if a node with that name exists already.
     */
    public TransientNodeState addNode(String name) throws ItemExistsException {
        NodeDelta child = getNodeDelta().addNode(name);
        return getNodeState(child);
    }

    /**
     * Transiently remove this node.
     * @throws javax.jcr.ItemNotFoundException if this node has been removed already
     */
    public void remove() throws ItemNotFoundException {
        getNodeStateProvider().release(getPath());
        getNodeDelta().getParent().removeNode(getName());
    }

    /**
     * Transiently move this node.
     *
     * @param name  name of this node at its {@code destination}
     * @param destination The destination of the move.
     * @throws javax.jcr.ItemExistsException  {@code name} exists at {@code destination}
     * @throws javax.jcr.PathNotFoundException  {@code destination} does not exist
     * @throws javax.jcr.ItemNotFoundException  {@code name} does not exist
     */
    public void move(String name, Path destination) throws ItemExistsException, PathNotFoundException,
            ItemNotFoundException {

        getNodeDelta().moveNode(name, destination);
        getNodeStateProvider().release(getPath().concat(name));
    }

    /**
     * Transiently set a property.
     * @param name  Name of the property.
     * @param value  Value of the property. Use {@code null} or {@code JsonAtom.NULL}
     *               to remove the property.
     */
    public void setProperty(String name, JsonValue value) {
        getNodeDelta().setValue(name, value);
    }

    /**
     * @return {@code true} iff this instance has child nodes.
     */
    public boolean hasNodes() {
        return getNodes().hasNext();
    }

    /**
     * @return Iterator of all child node states of this instance.
     */
    public Iterator<TransientNodeState> getNodes() {
        Iterator<? extends ChildNodeEntry> persistedEntries = Iterators.flatten(
            new PagedIterator<ChildNodeEntry>(BATCH_SIZE) {
                @Override
                protected Iterator<? extends ChildNodeEntry> getPage(long pos, int size) {
                    return getPersistentNodeState().getChildNodeEntries(pos, size).iterator();
                }
            });

        final NodeDelta delta = getNodeDelta();

        Iterator<ChildNodeEntry> unmodifiedEntries = Iterators.filter(persistedEntries,
            new Predicate<ChildNodeEntry>() {
                @Override
                public boolean evaluate(ChildNodeEntry entry) {
                    return !delta.isNodeModified(entry.getName());
                }
            });

        Iterator<TransientNodeState> unmodifiedStates = Iterators.map(unmodifiedEntries,
            new Function1<ChildNodeEntry, TransientNodeState>() {
                @Override
                public TransientNodeState apply(ChildNodeEntry entry) {
                    return getNodeState(delta.getNode(entry.getName()));
                }
            });

        Iterator<TransientNodeState> modifiedStates = Iterators.map(toIterable(delta.getNodes()).iterator(),
            new Function1<NodeDelta, TransientNodeState>() {
                @Override
                public TransientNodeState apply(NodeDelta delta) {
                    return getNodeState(delta);
                }
            });

        return Iterators.chain(unmodifiedStates, modifiedStates);
    }

    /**
     * @return {@code true} iff this instance has properties
     */
    public boolean hasProperties() {
        return getProperties().hasNext();
    }

    /**
     * @return Iterator of all property states of this instance.
     */
    public Iterator<PropertyState> getProperties() {
        Iterable<? extends PropertyState> propertyStates = getPersistentNodeState().getProperties();
        final NodeDelta delta = getNodeDelta();

        Iterator<PropertyState> propertyEntries =
            Iterators.filter(propertyStates.iterator(),
                new Predicate<PropertyState>() {
                    @Override
                    public boolean evaluate(PropertyState state) {
                        return !state.getName().startsWith(":") && !delta.hasProperty(state.getName());
                    }
                });

        Iterator<PropertyState> modifiedProperties = delta.getPropertyStates();
        return Iterators.chain(propertyEntries, Iterators.toIterable(modifiedProperties).iterator());
    }

    /**
     * @param name  name of the property
     * @return  value of the property named {@code name}.
     * @throws javax.jcr.ItemNotFoundException  if no such property exists.
     */
    public JsonValue getPropertyValue(String name) throws ItemNotFoundException {
        JsonValue value = getPropertyValueOrNull(name);
        if (value == null) {
            throw new ItemNotFoundException(name);
        }

        return value;
    }

    /**
     * @param name name of the property
     * @return {@code true} iff this instance has a property name {@code name}.
     */
    public boolean hasProperty(String name) {
        return getPropertyValueOrNull(name) != null;
    }

    /**
     * @param name name of the property
     * @return {@code true} iff the property named {@code name} has been transiently added.
     */
    public boolean isPropertyNew(String name) {
        JsonValue value = getNodeDelta().getPropertyValue(name);
        return value != null && !value.isNull() && getPersistedPropertyValue(name) == null;
    }

    /**
     * @param name name of the property
     * @return {@code true} iff the property named {@code name} has been transiently modified.
     */
    public boolean isPropertyModified(String name) {
        return getNodeDelta().hasProperty(name);
    }

    /**
     * Transiently remove a property.
     * @param name  name of the property to remove.
     */
    public void removeProperty(String name) {
        getNodeDelta().setValue(name, null);
    }

    @Override
    public String toString() {
        return "TransientNodeState(" + getPath().toString() + ')';
    }

    //------------------------------------------< private >---

    private NodeStateProvider getNodeStateProvider() {
        return sessionContext.getNodeStateProvider();
    }
    
    private TransientNodeState getNodeState(NodeDelta nodeDelta) {
        return getNodeStateProvider().getNodeState(nodeDelta);
    }

    private JsonValue getPropertyValueOrNull(String name) {
        JsonValue value = getNodeDelta().getPropertyValue(name);
        if (value == null) {
            return getPersistedPropertyValue(name);
        }
        else {
            return value == JsonAtom.NULL ? null : value;
        }
    }

    private JsonValue getPersistedPropertyValue(String name) {
        PropertyState state = getPersistentNodeState().getProperty(name);
        if (state == null) {
            return null;
        }
        else {
            return ((PropertyStateImpl) state).getValue();  // fixme: don't cast, see OAK-16
        }
    }

    private synchronized NodeState getPersistentNodeState() {
        Path path = getNodeDelta().getPersistentPath();
        String baseRevision = sessionContext.getRevision();
        if (persistentNodeState == null || !revision.equals(baseRevision)) {
            revision = baseRevision;
            if (path == null) {
                persistentNodeState = EmptyNodeState.INSTANCE;
            }
            else {
                persistentNodeState = new PersistentNodeState(sessionContext.getMicrokernel(), path, revision);
            }
        }

        return persistentNodeState;
    }

    private NodeDelta getNodeDelta() {
        return nodeDelta = getNodeStateProvider().getNodeDelta(nodeDelta.getPath());
    }

}
