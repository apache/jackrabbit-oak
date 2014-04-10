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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jcr.NamespaceException;
import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.core.config.UserManagerConfig;
import org.apache.jackrabbit.core.fs.FileSystem;
import org.apache.jackrabbit.core.fs.FileSystemException;
import org.apache.jackrabbit.core.nodetype.NodeTypeRegistry;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.security.authorization.PrivilegeRegistry;
import org.apache.jackrabbit.core.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.security.GroupEditorProvider;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.QItemDefinition;
import org.apache.jackrabbit.spi.QNodeDefinition;
import org.apache.jackrabbit.spi.QNodeTypeDefinition;
import org.apache.jackrabbit.spi.QPropertyDefinition;
import org.apache.jackrabbit.spi.QValue;
import org.apache.jackrabbit.spi.QValueConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
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
import static org.apache.jackrabbit.core.RepositoryImpl.ACTIVITIES_NODE_ID;
import static org.apache.jackrabbit.core.RepositoryImpl.ROOT_NODE_ID;
import static org.apache.jackrabbit.core.RepositoryImpl.VERSION_STORAGE_NODE_ID;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.name.Namespaces.addCustomMapping;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_AVAILABLE_QUERY_OPERATORS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_FULLTEXT_SEARCHABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_QUERYABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_QUERY_ORDERABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.NT_REP_PRIVILEGE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.NT_REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_AGGREGATES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_BITS;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_NEXT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_PRIVILEGES;
import static org.apache.jackrabbit.spi.commons.name.NameConstants.ANY_NAME;

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

    private boolean copyBinariesByReference = false;

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

    public boolean isCopyBinariesByReference() {
        return copyBinariesByReference;
    }

    public void setCopyBinariesByReference(boolean copyBinariesByReference) {
        this.copyBinariesByReference = copyBinariesByReference;
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
        RepositoryConfig config = source.getRepositoryConfig();
        logger.info(
                "Copying repository content from {} to Oak", config.getHomeDir());
        try {
            NodeBuilder builder = target.getRoot().builder();

            // init target repository first
            new InitialContent().initialize(builder);

            Map<String, String> uriToPrefix = newHashMap();
            Map<Integer, String> idxToPrefix = newHashMap();
            copyNamespaces(builder, uriToPrefix, idxToPrefix);
            copyNodeTypes(builder);
            copyPrivileges(builder);

            NodeState root = builder.getNodeState();
            copyVersionStore(builder, root, uriToPrefix, idxToPrefix);
            String workspaceName =
                    copyWorkspaces(builder, root, uriToPrefix, idxToPrefix);

            logger.info("Applying default commit hooks");
            String groupsPath;
            UserManagerConfig userConfig = config.getSecurityConfig().getSecurityManagerConfig().getUserManagerConfig();
            if (userConfig != null) {
                groupsPath = userConfig.getParameters().getProperty(UserManagerImpl.PARAM_GROUPS_PATH, UserConstants.DEFAULT_GROUP_PATH);
            } else {
                groupsPath = UserConstants.DEFAULT_GROUP_PATH;
            }
            // TODO: default hooks?
            List<CommitHook> hooks = newArrayList();
            hooks.add(new EditorHook(new CompositeEditorProvider(
                            new GroupEditorProvider(groupsPath),
                            new TypeEditorProvider(false),
                            new IndexUpdateProvider(new CompositeIndexEditorProvider(
                                    new ReferenceEditorProvider(),
                                    new PropertyIndexEditorProvider())))));
            hooks.addAll(new AuthorizationConfigurationImpl().getCommitHooks(workspaceName));
            target.merge(builder, CompositeHook.compose(hooks), CommitInfo.EMPTY);
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

    /**
     * Copies the registered namespaces to the target repository, and returns
     * the internal namespace index mapping used in bundle serialization.
     *
     * @param root root builder
     * @param uriToPrefix namespace URI to prefix mapping
     * @param idxToPrefix index to prefix mapping
     * @throws RepositoryException
     */
    private void copyNamespaces(
            NodeBuilder root,
            Map<String, String> uriToPrefix, Map<Integer, String> idxToPrefix)
            throws RepositoryException {
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder namespaces = system.child(NamespaceConstants.REP_NAMESPACES);

        Properties registry = loadProperties("/namespaces/ns_reg.properties");
        Properties indexes  = loadProperties("/namespaces/ns_idx.properties");

        for (String prefixHint : registry.stringPropertyNames()) {
            String prefix;
            String uri = registry.getProperty(prefixHint);
            if (".empty.key".equals(prefixHint)) {
                prefix = ""; // the default empty mapping is not stored
            } else {
                prefix = addCustomMapping(namespaces, uri, prefixHint);
            }

            String index = null;
            if (uri.isEmpty()) {
                index = indexes.getProperty(".empty.key");
            }
            if (index == null) {
                index = indexes.getProperty(uri);
            }

            Integer idx;
            if (index != null) {
                idx = Integer.decode(index);
            } else {
                int i = 0;
                do {
                    idx = (uri.hashCode() + i++) & 0x00ffffff;
                } while (idxToPrefix.containsKey(idx));
            }

            checkState(uriToPrefix.put(uri, prefix) == null);
            checkState(idxToPrefix.put(idx, prefix) == null);
        }

        Namespaces.buildIndexNode(namespaces);
    }

    private Properties loadProperties(String path) throws RepositoryException {
        Properties properties = new Properties();

        FileSystem filesystem = source.getFileSystem();
        try {
            if (filesystem.exists(path)) {
                InputStream stream = filesystem.getInputStream(path);
                try {
                    properties.load(stream);
                } finally {
                    stream.close();
                }
            }
        } catch (FileSystemException e) {
            throw new RepositoryException(e);
        } catch (IOException e) {
            throw new RepositoryException(e);
        }

        return properties;
    }

    @SuppressWarnings("deprecation")
    private void copyPrivileges(NodeBuilder root) throws RepositoryException {
        PrivilegeRegistry registry = source.getPrivilegeRegistry();
        NodeBuilder privileges = root.child(JCR_SYSTEM).child(REP_PRIVILEGES);
        privileges.setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGES, NAME);

        PrivilegeBits next = PrivilegeBits.NEXT_AFTER_BUILT_INS;

        logger.info("Copying registered privileges");
        for (Privilege privilege : registry.getRegisteredPrivileges()) {
            String name = privilege.getName();
            NodeBuilder def = privileges.child(name);
            def.setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGE, NAME);

            if (privilege.isAbstract()) {
                def.setProperty(REP_IS_ABSTRACT, true);
            }

            Privilege[] aggregate = privilege.getDeclaredAggregatePrivileges();
            if (aggregate.length > 0) {
                List<String> names = newArrayListWithCapacity(aggregate.length);
                for (Privilege p : aggregate) {
                    names.add(p.getName());
                }
                def.setProperty(REP_AGGREGATES, names, NAMES);
            }

            PrivilegeBits bits = PrivilegeBits.BUILT_IN.get(name);
            if (bits != null) {
                def.setProperty(bits.asPropertyState(REP_BITS));
            } else if (aggregate.length == 0) {
                bits = next;
                next = next.nextBits();
                def.setProperty(bits.asPropertyState(REP_BITS));
            }
        }

        privileges.setProperty(next.asPropertyState(REP_NEXT));

        // resolve privilege bits also for all aggregates
        for (String name : privileges.getChildNodeNames()) {
            resolvePrivilegeBits(privileges, name);
        }
    }

    private PrivilegeBits resolvePrivilegeBits(
            NodeBuilder privileges, String name) {
        NodeBuilder def = privileges.getChildNode(name);

        PropertyState b = def.getProperty(REP_BITS);
        if (b != null) {
            return PrivilegeBits.getInstance(b);
        }

        PrivilegeBits bits = PrivilegeBits.getInstance();
        for (String n : def.getNames(REP_AGGREGATES)) {
            bits.add(resolvePrivilegeBits(privileges, n));
        }
        def.setProperty(bits.asPropertyState(REP_BITS));
        return bits;
    }

    private void copyNodeTypes(NodeBuilder root) throws RepositoryException {
        NodeTypeRegistry sourceRegistry = source.getNodeTypeRegistry();
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder types = system.child(JCR_NODE_TYPES);

        logger.info("Copying registered node types");
        for (Name name : sourceRegistry.getRegisteredNodeTypes()) {
            String oakName = getOakName(name);
            // skip built-in nodetypes (OAK-1235)
            if (!types.hasChildNode(oakName)) {
                QNodeTypeDefinition def = sourceRegistry.getNodeTypeDef(name);
                NodeBuilder type = types.child(oakName);
                copyNodeType(def, type);
            }
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
            String name = JCR_PROPERTYDEFINITION + '[' + (i + 1) + ']';
            copyPropertyDefinition(properties[i], builder.child(name));
        }

        // + jcr:childNodeDefinition (nt:childNodeDefinition) = nt:childNodeDefinition protected sns
        QNodeDefinition[] childNodes = def.getChildNodeDefs();
        for (int i = 0; i < childNodes.length; i++) {
            String name = JCR_CHILDNODEDEFINITION + '[' + (i + 1) + ']';
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

    private void copyVersionStore(
            NodeBuilder builder, NodeState root,
            Map<String, String> uriToPrefix, Map<Integer, String> idxToPrefix)
            throws RepositoryException, IOException {
        logger.info("Copying version histories");

        PersistenceManager pm =
                source.getInternalVersionManager().getPersistenceManager();

        NodeBuilder system = builder.child(JCR_SYSTEM);
        system.setChildNode(JCR_VERSIONSTORAGE, new JackrabbitNodeState(
                pm, root, uriToPrefix, VERSION_STORAGE_NODE_ID,
                "/jcr:system/jcr:versionStorage", copyBinariesByReference));
        system.setChildNode("jcr:activities", new JackrabbitNodeState(
                pm, root, uriToPrefix, ACTIVITIES_NODE_ID,
                "/jcr:system/jcr:activities", copyBinariesByReference));
    }   

    private String copyWorkspaces(
            NodeBuilder builder, NodeState root,
            Map<String, String> uriToPrefix, Map<Integer, String> idxToPrefix)
            throws RepositoryException, IOException {
        logger.info("Copying default workspace");

        // Copy all the default workspace content
        RepositoryConfig config = source.getRepositoryConfig();
        String name = config.getDefaultWorkspaceName();

        PersistenceManager pm =
                source.getWorkspaceInfo(name).getPersistenceManager();

        NodeState state = new JackrabbitNodeState(
                pm, root, uriToPrefix, ROOT_NODE_ID, "/", copyBinariesByReference);
        for (PropertyState property : state.getProperties()) {
            builder.setProperty(property);
        }
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            String childName = child.getName();
            if (!JCR_SYSTEM.equals(childName)) {
                builder.setChildNode(childName, child.getNodeState());
            }
        }

        return name;
    }


}
