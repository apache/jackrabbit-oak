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
package org.apache.jackrabbit.oak.upgrade;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.JCR_AUTOCREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MANDATORY;
import static org.apache.jackrabbit.JcrConstants.JCR_MULTIPLE;
import static org.apache.jackrabbit.JcrConstants.JCR_NAME;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_ONPARENTVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYITEMNAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PROPERTYDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_PROTECTED;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDPRIMARYTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SAMENAMESIBLINGS;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VALUECONSTRAINTS;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.JcrConstants.NT_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.NT_NODETYPE;
import static org.apache.jackrabbit.JcrConstants.NT_PROPERTYDEFINITION;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_AVAILABLE_QUERY_OPERATORS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_FULLTEXT_SEARCHABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_QUERYABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_QUERY_ORDERABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.spi.commons.name.NameConstants.ANY_NAME;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.core.nodetype.NodeTypeRegistry;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.RegistrationEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.QItemDefinition;
import org.apache.jackrabbit.spi.QNodeDefinition;
import org.apache.jackrabbit.spi.QNodeTypeDefinition;
import org.apache.jackrabbit.spi.QPropertyDefinition;
import org.apache.jackrabbit.spi.QValue;
import org.apache.jackrabbit.spi.QValueConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class RepositoryUpgrade {

    /**
     * Logger instance
     */
    private static final Logger logger =
        LoggerFactory.getLogger(RepositoryUpgrade.class);

    /**
     * Source repository context.
     */
    private final RepositoryContext source;

    /**
     * Target node store.
     */
    private final NodeStore target;

    /**
     * Copies the contents of the repository in the given source directory
     * to the given target node store.
     *
     * @param source source repository directory
     * @param target target node store
     * @throws RepositoryException if the copy operation fails
     */
    public static void copy(File source, NodeStore target)
            throws RepositoryException {
        copy(RepositoryConfig.create(source), target);
    }

    /**
     * Copies the contents of the repository with the given configuration
     * to the given target node builder.
     *
     * @param source source repository configuration
     * @param target target node store
     * @throws RepositoryException if the copy operation fails
     */
    public static void copy(RepositoryConfig source, NodeStore target)
            throws RepositoryException {
        RepositoryContext context = RepositoryContext.create(source);
        try {
            new RepositoryUpgrade(context, target).copy();
        } finally {
            context.getRepository().shutdown();
        }
    }

    /**
     * Creates a tool for copying the full contents of the source repository
     * to the given target repository. Any existing content in the target
     * repository will be overwritten.
     *
     * @param source source repository context
     * @param target target node store
     */
    public RepositoryUpgrade(RepositoryContext source, NodeStore target) {
        this.source = source;
        this.target = target;
    }

    /**
     * Copies the full content from the source to the target repository.
     * <p>
     * The source repository <strong>must not be modified</strong> while
     * the copy operation is running to avoid an inconsistent copy.
     * <p>
     * This method leaves the search indexes of the target repository in
     * an 
     * Note that both the source and the target repository must be closed
     * during the copy operation as this method requires exclusive access
     * to the repositories.
     *
     * @throws RepositoryException if the copy operation fails
     */
    public void copy() throws RepositoryException {
        logger.info(
                "Copying repository content from {} to Oak",
                source.getRepositoryConfig().getHomeDir());
        try {
            NodeStoreBranch branch = target.branch();
            NodeBuilder builder = branch.getHead().builder();

            copyNamespaces(builder);
            copyNodeTypes(builder);
            copyVersionStore(builder);
            copyWorkspaces(builder);

            branch.setRoot(builder.getNodeState());
            branch.merge(new EditorHook(new RegistrationEditorProvider())); // TODO: default hooks?
        } catch (Exception e) {
            throw new RepositoryException("Failed to copy content", e);
        }
    }

    private String getOakName(Name name) throws NamespaceException {
        String uri = name.getNamespaceURI();
        String local = name.getLocalName();
        if (uri == null || uri.isEmpty()) {
            return local;
        } else {
            return source.getNamespaceRegistry().getPrefix(uri) + ":" + local;
        }
    }

    private void copyNamespaces(NodeBuilder root) throws RepositoryException {
        NamespaceRegistry sourceRegistry = source.getNamespaceRegistry();
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder namespaces = system.child("rep:namespaces");

        Set<String> defaults = ImmutableSet.of(
                NamespaceRegistry.NAMESPACE_EMPTY,
                NamespaceRegistry.NAMESPACE_JCR,
                NamespaceRegistry.NAMESPACE_MIX,
                NamespaceRegistry.NAMESPACE_NT,
                NamespaceRegistry.NAMESPACE_XML,
                NamespaceConstants.NAMESPACE_SV,
                NamespaceConstants.NAMESPACE_REP);
        logger.info("Copying registered namespaces");
        for (String uri : sourceRegistry.getURIs()) {
            if (!defaults.contains(uri)) {
                namespaces.setProperty(sourceRegistry.getPrefix(uri), uri);
            }
        }
    }

    private void copyNodeTypes(NodeBuilder root) throws RepositoryException {
        NodeTypeRegistry sourceRegistry = source.getNodeTypeRegistry();
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder types = system.child(JCR_NODE_TYPES);

        logger.info("Copying registered node types");
        for (Name name : sourceRegistry.getRegisteredNodeTypes()) {
            QNodeTypeDefinition def = sourceRegistry.getNodeTypeDef(name);
            NodeBuilder type = types.child(getOakName(name));
            copyNodeType(def, type);
        }
    }

    private void copyNodeType(QNodeTypeDefinition def, NodeBuilder builder)
            throws NamespaceException {
        builder.setProperty(JCR_PRIMARYTYPE, NT_NODETYPE, NAME);

        // - jcr:nodeTypeName (NAME) protected mandatory
        builder.setProperty(JCR_NODETYPENAME, getOakName(def.getName()), NAME);
        // - jcr:supertypes (NAME) protected multiple
        Name[] supertypes = def.getSupertypes();
        if (supertypes != null && supertypes.length > 0) {
            List<String> names = newArrayListWithCapacity(supertypes.length);
            for (Name supertype : supertypes) {
                names.add(getOakName(supertype));
            }
            builder.setProperty(JCR_SUPERTYPES, names, NAMES);
        }
        // - jcr:isAbstract (BOOLEAN) protected mandatory
        builder.setProperty(JCR_IS_ABSTRACT, def.isAbstract());
        // - jcr:isQueryable (BOOLEAN) protected mandatory
        builder.setProperty(JCR_IS_QUERYABLE, def.isQueryable());
        // - jcr:isMixin (BOOLEAN) protected mandatory
        builder.setProperty(JCR_ISMIXIN, def.isMixin());
        // - jcr:hasOrderableChildNodes (BOOLEAN) protected mandatory
        builder.setProperty(
                JCR_HASORDERABLECHILDNODES, def.hasOrderableChildNodes());
        // - jcr:primaryItemName (NAME) protected
        Name primary = def.getPrimaryItemName();
        if (primary != null) {
            builder.setProperty(
                    JCR_PRIMARYITEMNAME, getOakName(primary), NAME);
        }

        // + jcr:propertyDefinition (nt:propertyDefinition) = nt:propertyDefinition protected sns
        QPropertyDefinition[] properties = def.getPropertyDefs();
        for (int i = 0; i < properties.length; i++) {
            String name = JCR_PROPERTYDEFINITION + '[' + i + ']';
            copyPropertyDefinition(properties[i], builder.child(name));
        }

        // + jcr:childNodeDefinition (nt:childNodeDefinition) = nt:childNodeDefinition protected sns
        QNodeDefinition[] childNodes = def.getChildNodeDefs();
        for (int i = 0; i < childNodes.length; i++) {
            String name = JCR_CHILDNODEDEFINITION + '[' + i + ']';
            copyChildNodeDefinition(childNodes[i], builder.child(name));
        }
    }

    private void copyPropertyDefinition(
            QPropertyDefinition def, NodeBuilder builder)
            throws NamespaceException {
        builder.setProperty(JCR_PRIMARYTYPE, NT_PROPERTYDEFINITION, NAME);

        copyItemDefinition(def, builder);

        // - jcr:requiredType (STRING) protected mandatory
        //   < 'STRING', 'URI', 'BINARY', 'LONG', 'DOUBLE',
        //     'DECIMAL', 'BOOLEAN', 'DATE', 'NAME', 'PATH',
        //     'REFERENCE', 'WEAKREFERENCE', 'UNDEFINED'
        builder.setProperty(
                JCR_REQUIREDTYPE,
                Type.fromTag(def.getRequiredType(), false).toString());
        // - jcr:valueConstraints (STRING) protected multiple
        QValueConstraint[] constraints = def.getValueConstraints();
        if (constraints != null && constraints.length > 0) {
            List<String> strings = newArrayListWithCapacity(constraints.length);
            for (QValueConstraint constraint : constraints) {
                strings.add(constraint.getString());
            }
            builder.setProperty(JCR_VALUECONSTRAINTS, strings, STRINGS);
        }
        // - jcr:defaultValues (UNDEFINED) protected multiple
        QValue[] values = def.getDefaultValues();
        if (values != null) {
            // TODO
        }
        // - jcr:multiple (BOOLEAN) protected mandatory
        builder.setProperty(JCR_MULTIPLE, def.isMultiple());
        // - jcr:availableQueryOperators (NAME) protected mandatory multiple
        List<String> operators = asList(def.getAvailableQueryOperators());
        builder.setProperty(JCR_AVAILABLE_QUERY_OPERATORS, operators, NAMES);
        // - jcr:isFullTextSearchable (BOOLEAN) protected mandatory
        builder.setProperty(
                JCR_IS_FULLTEXT_SEARCHABLE, def.isFullTextSearchable());
        // - jcr:isQueryOrderable (BOOLEAN) protected mandatory
        builder.setProperty(JCR_IS_QUERY_ORDERABLE, def.isQueryOrderable());
    }

    private void copyChildNodeDefinition(
            QNodeDefinition def, NodeBuilder builder)
            throws NamespaceException {
        builder.setProperty(JCR_PRIMARYTYPE, NT_CHILDNODEDEFINITION, NAME);

        copyItemDefinition(def, builder);

        // - jcr:requiredPrimaryTypes (NAME) = 'nt:base' protected mandatory multiple
        Name[] types = def.getRequiredPrimaryTypes();
        List<String> names = newArrayListWithCapacity(types.length);
        for (Name type : types) {
            names.add(getOakName(type));
        }
        builder.setProperty(JCR_REQUIREDPRIMARYTYPES, names, NAMES);
        // - jcr:defaultPrimaryType (NAME) protected
        Name type = def.getDefaultPrimaryType();
        if (type != null) {
            builder.setProperty(JCR_DEFAULTPRIMARYTYPE, getOakName(type), NAME);
        }
        // - jcr:sameNameSiblings (BOOLEAN) protected mandatory
        builder.setProperty(JCR_SAMENAMESIBLINGS, def.allowsSameNameSiblings());
    }

    private void copyItemDefinition(QItemDefinition def, NodeBuilder builder)
            throws NamespaceException {
        // - jcr:name (NAME) protected
        Name name = def.getName();
        if (name != null && !name.equals(ANY_NAME)) {
            builder.setProperty(JCR_NAME, getOakName(name), NAME);
        }
        // - jcr:autoCreated (BOOLEAN) protected mandatory
        builder.setProperty(JCR_AUTOCREATED, def.isAutoCreated());
        // - jcr:mandatory (BOOLEAN) protected mandatory
        builder.setProperty(JCR_MANDATORY, def.isMandatory());
        // - jcr:onParentVersion (STRING) protected mandatory
        //   < 'COPY', 'VERSION', 'INITIALIZE', 'COMPUTE', 'IGNORE', 'ABORT'
        builder.setProperty(
                JCR_ONPARENTVERSION,
                OnParentVersionAction.nameFromValue(def.getOnParentVersion()));
        // - jcr:protected (BOOLEAN) protected mandatory
        builder.setProperty(JCR_PROTECTED, def.isProtected());
    }

    private void copyVersionStore(NodeBuilder root)
            throws RepositoryException, IOException {
        logger.info("Copying version histories");
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder versionStorage = system.child(JCR_VERSIONSTORAGE);
        NodeBuilder activities = system.child("jcr:activities");

        PersistenceCopier copier = new PersistenceCopier(
                source.getInternalVersionManager().getPersistenceManager(),
                source.getNamespaceRegistry(), target);
        copier.copy(RepositoryImpl.VERSION_STORAGE_NODE_ID, versionStorage);
        copier.copy(RepositoryImpl.ACTIVITIES_NODE_ID, activities);
    }   

    private void copyWorkspaces(NodeBuilder root)
            throws RepositoryException, IOException {
        logger.info("Copying default workspace");

        // Copy all the default workspace content
        RepositoryConfig config = source.getRepositoryConfig();
        String name = config.getDefaultWorkspaceName();
        PersistenceCopier copier = new PersistenceCopier(
                source.getWorkspaceInfo(name).getPersistenceManager(),
                source.getNamespaceRegistry(), target);
        copier.excludeNode(RepositoryImpl.SYSTEM_ROOT_NODE_ID);
        copier.copy(RepositoryImpl.ROOT_NODE_ID, root);

        // TODO: Copy all the active open-scoped locks
    }


}
