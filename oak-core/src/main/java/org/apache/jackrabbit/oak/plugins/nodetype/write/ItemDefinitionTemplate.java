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
package org.apache.jackrabbit.oak.plugins.nodetype.write;

import static org.apache.jackrabbit.JcrConstants.JCR_AUTOCREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_MANDATORY;
import static org.apache.jackrabbit.JcrConstants.JCR_NAME;
import static org.apache.jackrabbit.JcrConstants.JCR_ONPARENTVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_PROTECTED;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

/**
 * Base class for the node and property definition template implementations
 * in this package. Takes care of the shared item definition attributes and
 * manages mappings between JCR and Oak names.
 */
abstract class ItemDefinitionTemplate extends NamedTemplate
        implements ItemDefinition {

    private boolean residual = false;

    private boolean isAutoCreated = false;

    private int onParentVersion = OnParentVersionAction.COPY;

    private boolean isProtected = false;

    private boolean isMandatory = false;

    protected ItemDefinitionTemplate(NameMapper mapper) {
        super(mapper);
    }

    protected ItemDefinitionTemplate(
            NameMapper mapper, ItemDefinition definition)
            throws ConstraintViolationException {
        super(mapper, definition.getName());
        setProtected(definition.isProtected());
        setMandatory(definition.isMandatory());
        setAutoCreated(definition.isAutoCreated());
        setOnParentVersion(definition.getOnParentVersion());
    }

    /**
     * Writes the contents of this item definition to the given tree node.
     * Used when registering new node types.
     *
     * @param tree an {@code nt:propertyDefinition} or
     *             {@code nt:childNodeDefinition} node
     * @throws RepositoryException if this definition could not be written
     */
    void writeTo(Tree tree) throws RepositoryException {
        if (!residual) {
            String oakName = getOakName();
            if (oakName == null) {
                throw new RepositoryException("Unnamed item definition");
            }
            tree.setProperty(JCR_NAME, oakName, Type.NAME);
        } else {
            tree.removeProperty(JCR_NAME);
        }

        // TODO avoid (in validator?) unbounded recursive auto creation.
        // See 3.7.2.3.5 Chained Auto-creation (OAK-411)
        tree.setProperty(JCR_AUTOCREATED, isAutoCreated);
        tree.setProperty(JCR_MANDATORY, isMandatory);
        tree.setProperty(JCR_PROTECTED, isProtected);
        tree.setProperty(
                JCR_ONPARENTVERSION,
                OnParentVersionAction.nameFromValue(onParentVersion));
    }

    //------------------------------------------------------------< public >--

    /**
     * Returns the name of this template, or {@code null} if the name
     * has not yet been set. The special name "*" is used for residual
     * item definitions.
     *
     * @return JCR name, "*", or {@code null}
     */
    @Override @CheckForNull
    public String getName() {
        if (residual) {
            return NodeTypeConstants.RESIDUAL_NAME;
        } else {
            return super.getName();
        }
    }

    /**
     * Sets the name of this template. Use the special name "*" for a residual
     * item definition.
     *
     * @param jcrName JCR name, or "*"
     * @throws ConstraintViolationException if the name is invalid
     */
    @Override
    public void setName(@Nonnull String jcrName)
            throws ConstraintViolationException {
        residual = NodeTypeConstants.RESIDUAL_NAME.equals(jcrName);
        if (!residual) {
            super.setName(jcrName);
        }
    }

    /**
     * Returns {@code null} since an item definition template is not
     * attached to a live, already registered node type.
     *
     * @return {@code null}
     */
    @Override
    public NodeType getDeclaringNodeType() {
        return null;
    }

    @Override
    public boolean isAutoCreated() {
        return isAutoCreated;
    }

    public void setAutoCreated(boolean isAutoCreated) {
        this.isAutoCreated = isAutoCreated;
    }

    @Override
    public boolean isMandatory() {
        return isMandatory;
    }

    public void setMandatory(boolean isMandatory) {
        this.isMandatory = isMandatory;
    }

    @Override
    public int getOnParentVersion() {
        return onParentVersion;
    }

    public void setOnParentVersion(int onParentVersion) {
        OnParentVersionAction.nameFromValue(onParentVersion); // validate
        this.onParentVersion = onParentVersion;
    }

    @Override
    public boolean isProtected() {
        return isProtected;
    }

    public void setProtected(boolean isProtected) {
        this.isProtected = isProtected;
    }

}
