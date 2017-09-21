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

import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDPRIMARYTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_SAMENAMESIBLINGS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.RESIDUAL_NAME;

import java.util.Arrays;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NameMapper;

class NodeDefinitionTemplateImpl extends ItemDefinitionTemplate
        implements NodeDefinitionTemplate {

    private boolean allowSameNameSiblings = false;

    private String defaultPrimaryTypeOakName = null;

    private String[] requiredPrimaryTypeOakNames = null;

    public NodeDefinitionTemplateImpl(NameMapper mapper) {
        super(mapper);
    }

    public NodeDefinitionTemplateImpl(
            NameMapper mapper, NodeDefinition definition)
            throws ConstraintViolationException {
        super(mapper, definition);
        setSameNameSiblings(definition.allowsSameNameSiblings());
        setDefaultPrimaryTypeName(definition.getDefaultPrimaryTypeName());
        setRequiredPrimaryTypeNames(definition.getRequiredPrimaryTypeNames());
    }

    /**
     * Writes the contents of this node definition to the given tree node.
     * Used when registering new node types.
     *
     * @param tree an {@code nt:childNodeDefinition} node
     * @throws RepositoryException if this definition could not be written
     */
    @Override
    void writeTo(Tree tree) throws RepositoryException {
        super.writeTo(tree);

        tree.setProperty(JCR_SAMENAMESIBLINGS, allowSameNameSiblings);

        if (requiredPrimaryTypeOakNames != null) {
            tree.setProperty(
                    JCR_REQUIREDPRIMARYTYPES,
                    Arrays.asList(requiredPrimaryTypeOakNames), Type.NAMES);
        } else {
            tree.removeProperty(JCR_REQUIREDPRIMARYTYPES);
        }

        if (defaultPrimaryTypeOakName != null) {
            tree.setProperty(
                    JCR_DEFAULTPRIMARYTYPE,
                    defaultPrimaryTypeOakName, Type.NAME);
        } else {
            tree.removeProperty(JCR_DEFAULTPRIMARYTYPE);
        }
    }

    //------------------------------------------------------------< public >--

    @Override
    public boolean allowsSameNameSiblings() {
        return allowSameNameSiblings;
    }

    @Override
    public void setSameNameSiblings(boolean allowSameNameSiblings) {
        this.allowSameNameSiblings = allowSameNameSiblings;
    }

    /**
     * Returns {@code null} since an item definition template is not
     * attached to a live, already registered node type.
     *
     * @return {@code null}
     */
    @Override
    public NodeType getDefaultPrimaryType() {
        return null;
    }

    @Override
    public String getDefaultPrimaryTypeName() {
        return getJcrNameAllowNull(defaultPrimaryTypeOakName);
    }

    @Override
    public void setDefaultPrimaryTypeName(String jcrName)
            throws ConstraintViolationException {
        this.defaultPrimaryTypeOakName =
                    getOakNameAllowNullOrThrowConstraintViolation(jcrName);
    }

    /**
     * Returns {@code null} since an item definition template is not
     * attached to a live, already registered node type.
     *
     * @return {@code null}
     */
    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        return null;
    }

    @Override
    public String[] getRequiredPrimaryTypeNames() {
        return getJcrNamesAllowNull(requiredPrimaryTypeOakNames);
    }

    @Override
    public void setRequiredPrimaryTypeNames(String[] jcrNames)
            throws ConstraintViolationException {
        this.requiredPrimaryTypeOakNames =
                getOakNamesOrThrowConstraintViolation(jcrNames);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("+ ");
        if (getOakName() == null) {
            builder.append(RESIDUAL_NAME);
        } else {
            builder.append(getOakName());
        }
        if (defaultPrimaryTypeOakName != null) {
            builder.append(" (");
            builder.append(defaultPrimaryTypeOakName);
            builder.append(")");
        }
        if (isAutoCreated()) {
            builder.append(" a");
        }
        if (isProtected()) {
            builder.append(" p");
        }
        if (isMandatory()) {
            builder.append(" m");
        }
        if (getOnParentVersion() != OnParentVersionAction.COPY) {
            builder.append(" ");
            builder.append(OnParentVersionAction.nameFromValue(getOnParentVersion()));
        }
        return builder.toString();
    }

}
