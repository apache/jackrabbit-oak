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
package org.apache.jackrabbit.oak.plugins.nodetype;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.oak.namepath.NameMapper;

/**
 * Base class for the node and property definition template implementations
 * in this package. Takes care of the shared item definition attributes and
 * manages mappings between JCR and Oak names.
 */
abstract class ItemDefinitionTemplateImpl extends AbstractNamedTemplate
        implements ItemDefinition {

    private boolean residual = false;

    private boolean isAutoCreated = false;

    private int onParentVersion = OnParentVersionAction.COPY;

    protected boolean isProtected = false;

    protected boolean isMandatory = false;

    protected ItemDefinitionTemplateImpl(NameMapper mapper) {
        super(mapper);
    }

    protected ItemDefinitionTemplateImpl(
            NameMapper mapper, ItemDefinition definition)
            throws ConstraintViolationException {
        super(mapper, definition.getName());
        setProtected(definition.isProtected());
        setMandatory(definition.isMandatory());
        setAutoCreated(definition.isAutoCreated());
        setOnParentVersion(definition.getOnParentVersion());
    }

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
