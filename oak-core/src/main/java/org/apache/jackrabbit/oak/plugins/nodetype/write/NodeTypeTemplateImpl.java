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

import static com.google.common.collect.Iterables.filter;
import static org.apache.jackrabbit.JcrConstants.JCR_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYITEMNAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PROPERTYDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
import static org.apache.jackrabbit.JcrConstants.NT_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.NT_NODETYPE;
import static org.apache.jackrabbit.JcrConstants.NT_PROPERTYDEFINITION;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_IS_QUERYABLE;

import java.util.Arrays;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeExistsException;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NameMapper;

class NodeTypeTemplateImpl extends NamedTemplate implements NodeTypeTemplate {

    private static final PropertyDefinition[] EMPTY_PROPERTY_DEFINITION_ARRAY =
            new PropertyDefinition[0];

    private static final NodeDefinition[] EMPTY_NODE_DEFINITION_ARRAY =
            new NodeDefinition[0];

    protected boolean isMixin;

    protected boolean isOrderable;

    protected boolean isAbstract;

    protected boolean queryable;

    private String primaryItemOakName = null; // not defined by default

    @Nonnull
    private String[] superTypeOakNames = new String[0];

    private List<PropertyDefinitionTemplateImpl> propertyDefinitionTemplates = null;

    private List<NodeDefinitionTemplateImpl> nodeDefinitionTemplates = null;

    NodeTypeTemplateImpl(@Nonnull NameMapper mapper) {
        super(mapper);
    }

    NodeTypeTemplateImpl(@Nonnull NameMapper mapper, @Nonnull NodeTypeDefinition definition)
            throws ConstraintViolationException {
        super(mapper, definition.getName());

        setMixin(definition.isMixin());
        setOrderableChildNodes(definition.hasOrderableChildNodes());
        setAbstract(definition.isAbstract());
        setQueryable(definition.isQueryable());
        String primaryItemName = definition.getPrimaryItemName();
        if (primaryItemName != null) {
            setPrimaryItemName(primaryItemName);
        }
        setDeclaredSuperTypeNames(definition.getDeclaredSupertypeNames());

        PropertyDefinition[] pds = definition.getDeclaredPropertyDefinitions();
        if (pds != null) {
            propertyDefinitionTemplates =
                    Lists.newArrayListWithCapacity(pds.length);
            for (PropertyDefinition pd : pds) {
                propertyDefinitionTemplates.add(
                        new PropertyDefinitionTemplateImpl(mapper, pd));
            }
        }

        NodeDefinition[] nds = definition.getDeclaredChildNodeDefinitions();
        if (nds != null) {
            nodeDefinitionTemplates =
                    Lists.newArrayListWithCapacity(nds.length);
            for (NodeDefinition nd : nds) {
                nodeDefinitionTemplates.add(
                        new NodeDefinitionTemplateImpl(mapper, nd));
            }
        }
    }

    /**
     * Writes this node type as an {@code nt:nodeType} child of the given
     * parent node. An exception is thrown if the child node already exists,
     * unless the {@code allowUpdate} flag is set, in which case the existing
     * node is overwritten.
     *
     * @param parent parent node under which to write this node type
     * @param allowUpdate whether to overwrite an existing type
     * @return The node type tree.
     * @throws RepositoryException if this type could not be written
     */
    Tree writeTo(@Nonnull Tree parent, boolean allowUpdate) throws RepositoryException {
        String oakName = getOakName();
        if (oakName == null) {
            throw new RepositoryException("Cannot register node type: name is missing.");
        }

        Tree type = parent.getChild(oakName);
        if (!type.exists()) {
            type = parent.addChild(oakName);
            type.setProperty(JCR_PRIMARYTYPE, NT_NODETYPE, Type.NAME);
        } else if (!allowUpdate) {
            throw new NodeTypeExistsException(
                    "Node type " + getName() + " already exists");
        }

        type.setProperty(JCR_NODETYPENAME, oakName, Type.NAME);

        if (superTypeOakNames.length > 0) {
            type.setProperty(
                    JCR_SUPERTYPES,
                    Arrays.asList(superTypeOakNames), Type.NAMES);
        } else {
            type.removeProperty(JCR_SUPERTYPES);
        }

        type.setProperty(JCR_IS_ABSTRACT, isAbstract);
        type.setProperty(JCR_IS_QUERYABLE, queryable);
        type.setProperty(JCR_ISMIXIN, isMixin);

        // TODO fail (in validator?) if not orderable but a supertype is orderable
        // See 3.7.6.7 Node Type Attribute Subtyping Rules (OAK-411)
        type.setProperty(JCR_HASORDERABLECHILDNODES, isOrderable);

        // TODO fail (in validator?) if a supertype specifies a different primary item
        // See 3.7.6.7 Node Type Attribute Subtyping Rules (OAK-411)
        if (primaryItemOakName != null) {
            type.setProperty(JCR_PRIMARYITEMNAME, primaryItemOakName, Type.NAME);
        } else {
            type.removeProperty(JCR_PRIMARYITEMNAME);
        }

        // TODO fail (in validator?) on invalid item definitions
        // See 3.7.6.8 Item Definitions in Subtypes (OAK-411)
        writeItemDefinitions(type, propertyDefinitionTemplates, JCR_PROPERTYDEFINITION, NT_PROPERTYDEFINITION);
        writeItemDefinitions(type, nodeDefinitionTemplates, JCR_CHILDNODEDEFINITION, NT_CHILDNODEDEFINITION);

        return type;
    }

    private static void writeItemDefinitions(@Nonnull Tree nodeTypeTree, @CheckForNull List<? extends ItemDefinitionTemplate> itemDefTemplates,
                                             @Nonnull String nodeName, @Nonnull String primaryTypeName) throws RepositoryException {
        // first remove existing
        for (Tree t : filter(nodeTypeTree.getChildren(), new SameNamePredicate(nodeName))) {
            t.remove();
        }
        // now write definitions
        int index = 1;
        if (itemDefTemplates != null) {
            for (ItemDefinitionTemplate template : itemDefTemplates) {
                String name = nodeName(nodeName, index);
                Tree tree = nodeTypeTree.getChild(name);
                if (!tree.exists()) {
                    tree = nodeTypeTree.addChild(name);
                    tree.setProperty(
                            JCR_PRIMARYTYPE, primaryTypeName, NAME);
                }
                template.writeTo(tree);
                index++;
            }
        }
    }

    private static String nodeName(String name, int index) {
        return (index == 1) ? name : name + '[' + index + ']';
    }

    private static final class SameNamePredicate implements Predicate<Tree> {

        private final String name;

        private SameNamePredicate(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Tree t) {
            String s = t.getName();
            return s.equals(name) || s.startsWith(name + "[");
        }
    }

    //------------------------------------------------------------< public >--

    @Override
    public boolean isMixin() {
        return isMixin;
    }

    @Override
    public void setMixin(boolean mixin) {
        this.isMixin = mixin;
    }

    @Override
    public boolean hasOrderableChildNodes() {
        return isOrderable ;
    }

    @Override
    public void setOrderableChildNodes(boolean orderable) {
        this.isOrderable = orderable;
    }

    @Override
    public boolean isAbstract() {
        return isAbstract;
    }

    @Override
    public void setAbstract(boolean abstractStatus) {
        this.isAbstract = abstractStatus;
    }

    @Override
    public boolean isQueryable() {
        return queryable;
    }

    @Override
    public void setQueryable(boolean queryable) {
        this.queryable = queryable;
    }

    @Override
    public String getPrimaryItemName() {
        return getJcrNameAllowNull(primaryItemOakName);
    }

    @Override
    public void setPrimaryItemName(String jcrName)
            throws ConstraintViolationException {
        this.primaryItemOakName =
                getOakNameAllowNullOrThrowConstraintViolation(jcrName);
    }

    @Override
    public String[] getDeclaredSupertypeNames() {
        return getJcrNamesAllowNull(superTypeOakNames);
    }

    @Override
    public void setDeclaredSuperTypeNames(String[] jcrNames)
            throws ConstraintViolationException {
        this.superTypeOakNames =
                getOakNamesOrThrowConstraintViolation(jcrNames);
    }

    @Override
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        if (propertyDefinitionTemplates != null) {
            return propertyDefinitionTemplates.toArray(
                    EMPTY_PROPERTY_DEFINITION_ARRAY);
        } else {
            return null;
        }
    }

    @Override
    public List<? extends PropertyDefinitionTemplate> getPropertyDefinitionTemplates() {
        if (propertyDefinitionTemplates == null) {
            propertyDefinitionTemplates = Lists.newArrayList();
        }
        return propertyDefinitionTemplates;
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        if (nodeDefinitionTemplates != null) {
            return nodeDefinitionTemplates.toArray(
                    EMPTY_NODE_DEFINITION_ARRAY);
        } else {
            return null;
        }
    }

    @Override
    public List<? extends NodeDefinitionTemplate> getNodeDefinitionTemplates() {
        if (nodeDefinitionTemplates == null) {
            nodeDefinitionTemplates = Lists.newArrayList();
        }
        return nodeDefinitionTemplates;
    }

    //------------------------------------------------------------< Object >--

    public String toString() {
        return String.format("NodeTypeTemplate(%s)", getOakName());
    }

}