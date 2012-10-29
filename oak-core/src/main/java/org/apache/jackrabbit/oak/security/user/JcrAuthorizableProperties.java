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
package org.apache.jackrabbit.oak.security.user;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JcrAuthorizableProperty... TODO
 */
class JcrAuthorizableProperties implements AuthorizableProperties, UserConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(JcrAuthorizableProperties.class);

    private final Node authorizableNode;
    private final NamePathMapper namePathMapper;

    JcrAuthorizableProperties(Node authorizableNode, NamePathMapper namePathMapper) {
        this.authorizableNode = authorizableNode;
        this.namePathMapper = namePathMapper;
    }

    @Override
    public Iterator<String> getNames(String relPath) throws RepositoryException {
        Node node = getNode();
        Node n = node.getNode(relPath);
        if (Text.isDescendantOrEqual(node.getPath(), n.getPath())) {
            List<String> l = new ArrayList<String>();
            for (PropertyIterator it = n.getProperties(); it.hasNext();) {
                Property prop = it.nextProperty();
                if (isAuthorizableProperty(prop, false)) {
                    l.add(prop.getName());
                }
            }
            return l.iterator();
        } else {
            throw new IllegalArgumentException("Relative path " + relPath + " refers to items outside of scope of authorizable.");
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#hasProperty(String)
     */
    @Override
    public boolean hasProperty(String relPath) throws RepositoryException {
        Node node = getNode();
        return node.hasProperty(relPath) && isAuthorizableProperty(node.getProperty(relPath), true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getProperty(String)
     */
    @Override
    public Value[] getProperty(String relPath) throws RepositoryException {
        Node node = getNode();
        Value[] values = null;
        if (node.hasProperty(relPath)) {
            Property prop = node.getProperty(relPath);
            if (isAuthorizableProperty(prop, true)) {
                if (prop.isMultiple()) {
                    values = prop.getValues();
                } else {
                    values = new Value[]{prop.getValue()};
                }
            }
        }
        return values;
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value)
     */
    @Override
    public void setProperty(String relPath, Value value) throws RepositoryException {
        String name = Text.getName(relPath);
        String intermediate = (relPath.equals(name)) ? null : Text.getRelativeParent(relPath, 1);

        Node n = getOrCreateTargetNode(intermediate);
        // check if the property has already been created as multi valued
        // property before -> in this case remove in order to avoid
        // ValueFormatException.
        if (n.hasProperty(name)) {
            Property p = n.getProperty(name);
            if (p.isMultiple()) {
                p.remove();
            }
        }
        n.setProperty(name, value);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value[])
     */
    @Override
    public void setProperty(String relPath, Value[] values) throws RepositoryException {
        String name = Text.getName(relPath);
        String intermediate = (relPath.equals(name)) ? null : Text.getRelativeParent(relPath, 1);

        Node n = getOrCreateTargetNode(intermediate);
        // check if the property has already been created as single valued
        // property before -> in this case remove in order to avoid
        // ValueFormatException.
        if (n.hasProperty(name)) {
            Property p = n.getProperty(name);
            if (!p.isMultiple()) {
                p.remove();
            }
        }
        n.setProperty(name, values);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#removeProperty(String)
     */
    @Override
    public boolean removeProperty(String relPath) throws RepositoryException {
        Node node = getNode();
        if (node.hasProperty(relPath)) {
            Property p = node.getProperty(relPath);
            if (isAuthorizableProperty(p, true)) {
                p.remove();
                return true;
            } else {
                throw new ConstraintViolationException("Property " + relPath + " isn't a modifiable authorizable property");
            }
        }
        // no such property or wasn't a property of this authorizable.
        return false;
    }

    private Node getNode() {
        return authorizableNode;
    }

    private String getJcrName(String oakName) {
        return namePathMapper.getJcrName(oakName);
    }

    /**
     * Returns true if the given property of the authorizable node is one of the
     * non-protected properties defined by the rep:Authorizable node type or a
     * some other descendant of the authorizable node.
     *
     * @param prop Property to be tested.
     * @param verifyAncestor If true the property is tested to be a descendant
     * of the node of this authorizable; otherwise it is expected that this
     * test has been executed by the caller.
     * @return {@code true} if the given property is defined
     * by the rep:authorizable node type or one of it's sub-node types;
     * {@code false} otherwise.
     * @throws RepositoryException If the property definition cannot be retrieved.
     */
    private boolean isAuthorizableProperty(Property prop, boolean verifyAncestor) throws RepositoryException {
        Node node = getNode();
        if (verifyAncestor && !Text.isDescendant(node.getPath(), prop.getPath())) {
            log.debug("Attempt to access property outside of authorizable scope.");
            return false;
        }

        PropertyDefinition def = prop.getDefinition();
        if (def.isProtected()) {
            return false;
        } else if (node.isSame(prop.getParent())) {
            NodeType declaringNt = prop.getDefinition().getDeclaringNodeType();
            return declaringNt.isNodeType(getJcrName(NT_REP_AUTHORIZABLE));
        } else {
            // another non-protected property somewhere in the subtree of this
            // authorizable node -> is a property that can be set using #setProperty.
            return true;
        }
    }

    /**
     * Retrieves the node at {@code relPath} relative to node associated with
     * this authorizable. If no such node exist it and any missing intermediate
     * nodes are created.
     *
     * @param relPath A relative path.
     * @return The corresponding node.
     * @throws RepositoryException If an error occurs or if {@code relPath} refers
     * to a node that is outside of the scope of this authorizable.
     */
    @Nonnull
    private Node getOrCreateTargetNode(String relPath) throws RepositoryException {
        Node n;
        Node node = getNode();
        if (relPath != null) {
            String userPath = node.getPath();
            if (node.hasNode(relPath)) {
                n = node.getNode(relPath);
                if (!Text.isDescendantOrEqual(userPath, n.getPath())) {
                    throw new RepositoryException("Relative path " + relPath + " outside of scope of " + this);
                }
            } else {
                n = node;
                for (String segment : Text.explode(relPath, '/')) {
                    if (n.hasNode(segment)) {
                        n = n.getNode(segment);
                    } else {
                        if (Text.isDescendantOrEqual(userPath, n.getPath())) {
                            n = n.addNode(segment);
                        } else {
                            throw new RepositoryException("Relative path " + relPath + " outside of scope of " + this);
                        }
                    }
                }
            }
        } else {
            n = node;
        }
        return n;
    }
}