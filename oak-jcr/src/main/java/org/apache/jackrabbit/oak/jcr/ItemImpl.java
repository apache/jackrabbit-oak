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
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Item;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.jackrabbit.commons.AbstractItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.jcr.PropertyType.UNDEFINED;

/**
 * {@code ItemImpl}...
 */
abstract class ItemImpl<T extends ItemDelegate> extends AbstractItem {

    protected final SessionDelegate sessionDelegate;
    protected final T dlg;

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ItemImpl.class);

    protected ItemImpl(SessionDelegate sessionDelegate, T itemDelegate) {
        this.sessionDelegate = sessionDelegate;
        this.dlg = itemDelegate;
    }

    //---------------------------------------------------------------< Item >---

    /**
     * @see javax.jcr.Item#getName()
     */
    @Override
    @Nonnull
    public String getName() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>() {
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
        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return toJcrPath(dlg.getPath());
            }
        });
    }

    @Override
    @Nonnull
    public Session getSession() throws RepositoryException {
        return sessionDelegate.getSession();
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

    //------------------------------------------------------------< internal >---

    /**
     * Returns all the node types of the given node, in a breadth-first
     * traversal order of the type hierarchy.
     * <p>
     * This utility method used by the getDefinition() methods in the
     * {@link PropertyImpl} and {@link NodeImpl} subclasses.
     *
     * @param node node instance
     * @return all types of the given node
     * @throws RepositoryException if the type information can not be accessed
     */
    protected static Iterable<NodeType> getAllNodeTypes(Node node)
            throws RepositoryException {
        Map<String, NodeType> types = Maps.newHashMap();

        Queue<NodeType> queue = Queues.newArrayDeque();
        queue.add(node.getPrimaryNodeType());
        queue.addAll(Arrays.asList(node.getMixinNodeTypes()));
        while (!queue.isEmpty()) {
            NodeType type = queue.remove();
            String name = type.getName();
            if (!types.containsKey(name)) {
                types.put(name, type);
                queue.addAll(Arrays.asList(type.getDeclaredSupertypes()));
            }
        }

        return types.values();
    }

    // FIXME: move to node type utility (oak-nodetype-plugin)
    protected static PropertyDefinition getPropertyDefinition(Node parent,
                                                              String propName,
                                                              boolean isMultiple,
                                                              int type,
                                                              boolean exactTypeMatch) throws RepositoryException {
        // TODO: This may need to be optimized
        for (NodeType nt : getAllNodeTypes(parent)) {
            for (PropertyDefinition def : nt.getDeclaredPropertyDefinitions()) {
                String defName = def.getName();
                int defType = def.getRequiredType();
                if (propName.equals(defName)
                        && isMultiple == def.isMultiple()
                        &&(!exactTypeMatch || (type == defType || UNDEFINED == type || UNDEFINED == defType))) {
                    return def;
                }
            }
        }

        // try if there is a residual definition
        for (NodeType nt : getAllNodeTypes(parent)) {
            for (PropertyDefinition def : nt.getDeclaredPropertyDefinitions()) {
                String defName = def.getName();
                int defType = def.getRequiredType();
                if ("*".equals(defName)
                        && isMultiple == def.isMultiple()
                        && (!exactTypeMatch || (type == defType || UNDEFINED == type || UNDEFINED == defType))) {
                    return def;
                }
            }
        }

        // FIXME: Shouldn't be needed
        for (NodeType nt : getAllNodeTypes(parent)) {
            for (PropertyDefinition def : nt.getDeclaredPropertyDefinitions()) {
                String defName = def.getName();
                if ((propName.equals(defName) || "*".equals(defName))
                        && type == PropertyType.STRING
                        && isMultiple == def.isMultiple()) {
                    return def;
                }
            }
        }
        throw new RepositoryException("No matching property definition found for " + propName);
    }


    /**
     * Performs a sanity check on this item and the associated session.
     *
     * @throws RepositoryException if this item has been rendered invalid for some reason
     */
    void checkStatus() throws RepositoryException {
        if (dlg.isStale()) {
            throw new InvalidItemStateException("stale");
        }

        // check session status
        if (!sessionDelegate.isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }

        // TODO: validate item state.
    }

    void checkProtected() throws RepositoryException {
        ItemDefinition definition = (isNode()) ? ((Node) this).getDefinition() : ((Property) this).getDefinition();
        checkProtected(definition);
    }

    void checkProtected(ItemDefinition definition) throws RepositoryException {
        if (definition.isProtected()) {
            throw new ConstraintViolationException("Item is protected.");
        }
    }

    /**
     * Ensure that the associated session has no pending changes and throw an
     * exception otherwise.
     *
     * @throws InvalidItemStateException if this nodes session has pending changes
     * @throws RepositoryException
     */
    void ensureNoPendingSessionChanges() throws RepositoryException {
        // check for pending changes
        if (sessionDelegate.hasPendingChanges()) {
            String msg = "Unable to perform operation. Session has pending changes.";
            log.debug(msg);
            throw new InvalidItemStateException(msg);
        }
    }

    /**
     * Returns the value factory associated with the editing session.
     *
     * @return the value factory
     */
    @Nonnull
    ValueFactory getValueFactory() {
        return sessionDelegate.getValueFactory();
    }

    @Nonnull
    String toJcrPath(String oakPath) {
        return sessionDelegate.getNamePathMapper().getJcrPath(oakPath);
    }
}
