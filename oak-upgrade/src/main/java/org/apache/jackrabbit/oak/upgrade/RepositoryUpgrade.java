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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.core.RepositoryImpl.ACTIVITIES_NODE_ID;
import static org.apache.jackrabbit.core.RepositoryImpl.ROOT_NODE_ID;
import static org.apache.jackrabbit.core.RepositoryImpl.VERSION_STORAGE_NODE_ID;
import static org.apache.jackrabbit.oak.plugins.name.Namespaces.addCustomMapping;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.spi.commons.name.NameConstants.ANY_NAME;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.NamespaceException;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.security.Privilege;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.BeanConfig;
import org.apache.jackrabbit.core.config.LoginModuleConfig;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.core.config.SecurityConfig;
import org.apache.jackrabbit.core.fs.FileSystem;
import org.apache.jackrabbit.core.fs.FileSystemException;
import org.apache.jackrabbit.core.nodetype.NodeTypeRegistry;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.security.authorization.PrivilegeRegistry;
import org.apache.jackrabbit.core.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.ProgressNotificationEditor;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.security.GroupEditorProvider;
import org.apache.jackrabbit.oak.upgrade.security.RestrictionEditorProvider;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.QNodeDefinition;
import org.apache.jackrabbit.spi.QNodeTypeDefinition;
import org.apache.jackrabbit.spi.QPropertyDefinition;
import org.apache.jackrabbit.spi.QValue;
import org.apache.jackrabbit.spi.QValueConstraint;
import org.apache.jackrabbit.spi.commons.conversion.DefaultNamePathResolver;
import org.apache.jackrabbit.spi.commons.conversion.NamePathResolver;
import org.apache.jackrabbit.spi.commons.value.ValueFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.log.LogInputStream;

public class RepositoryUpgrade {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryUpgrade.class);

    /**
     * Source repository context.
     */
    private final RepositoryContext source;

    /**
     * Target node store.
     */
    private final NodeStore target;

    private boolean copyBinariesByReference = false;

    private List<CommitHook> customCommitHooks = null;

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
            new RepositoryUpgrade(context, target).copy(null);
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
     * Returns the list of custom CommitHooks to be applied before the final
     * type validation, reference and indexing hooks.
     *
     * @return the list of custom CommitHooks
     */
    public List<CommitHook> getCustomCommitHooks() {
        return customCommitHooks;
    }

    /**
     * Sets the list of custom CommitHooks to be applied before the final
     * type validation, reference and indexing hooks.
     *
     * @param customCommitHooks the list of custom CommitHooks
     */
    public void setCustomCommitHooks(List<CommitHook> customCommitHooks) {
        this.customCommitHooks = customCommitHooks;
    }

    /**
     * Copies the full content from the source to the target repository.
     * <p>
     * The source repository <strong>must not be modified</strong> while
     * the copy operation is running to avoid an inconsistent copy.
     * <p>
     * Note that both the source and the target repository must be closed
     * during the copy operation as this method requires exclusive access
     * to the repositories.
     *
     * @param initializer optional extra repository initializer to use
     * @throws RepositoryException if the copy operation fails
     */
    public void copy(RepositoryInitializer initializer) throws RepositoryException {
        RepositoryConfig config = source.getRepositoryConfig();
        logger.info("Copying repository content from {} to Oak", config.getHomeDir());
        try {
            NodeState base = target.getRoot();
            NodeBuilder builder = base.builder();
            final Root upgradeRoot = new UpgradeRoot(builder);

            String workspaceName =
                    source.getRepositoryConfig().getDefaultWorkspaceName();
            SecurityProviderImpl security = new SecurityProviderImpl(
                    mapSecurityConfig(config.getSecurityConfig()));

            // init target repository first
            logger.info("Initializing initial repository content", config.getHomeDir());
            new InitialContent().initialize(builder);
            if (initializer != null) {
                initializer.initialize(builder);
            }
            logger.debug("InitialContent completed", config.getHomeDir());

            for (SecurityConfiguration sc : security.getConfigurations()) {
                RepositoryInitializer ri = sc.getRepositoryInitializer();
                ri.initialize(builder);
                logger.debug("Repository initializer '" + ri.getClass().getName() + "' completed", config.getHomeDir());
            }
            for (SecurityConfiguration sc : security.getConfigurations()) {
                WorkspaceInitializer wi = sc.getWorkspaceInitializer();
                wi.initialize(builder, workspaceName);
                logger.debug("Workspace initializer '" + wi.getClass().getName() + "' completed", config.getHomeDir());
            }

            HashBiMap<String, String> uriToPrefix = HashBiMap.create();
            logger.info("Copying registered namespaces");
            copyNamespaces(builder, uriToPrefix);
            logger.debug("Namespace registration completed.");

            logger.info("Copying registered node types");
            NodeTypeManager ntMgr = new ReadWriteNodeTypeManager() {
                @Override
                protected Tree getTypes() {
                    return upgradeRoot.getTree(NODE_TYPES_PATH);
                }

                @Nonnull
                @Override
                protected Root getWriteRoot() {
                    return upgradeRoot;
                }
            };
            copyNodeTypes(ntMgr, new ValueFactoryImpl(upgradeRoot, NamePathMapper.DEFAULT));
            logger.debug("Node type registration completed.");

            // migrate privileges
            logger.info("Copying registered privileges");
            PrivilegeConfiguration privilegeConfiguration = security.getConfiguration(PrivilegeConfiguration.class);
            copyCustomPrivileges(privilegeConfiguration.getPrivilegeManager(upgradeRoot, NamePathMapper.DEFAULT));
            logger.debug("Privilege registration completed.");

            // Triggers compilation of type information, which we need for
            // the type predicates used by the bulk  copy operations below.
            new TypeEditorProvider(false).getRootEditor(
                    base, builder.getNodeState(), builder, null);

            Map<String, String> versionablePaths = newHashMap();
            NodeState root = builder.getNodeState();

            logger.info("Copying workspace content");
            copyWorkspace(builder, root, workspaceName, uriToPrefix, versionablePaths);
            logger.debug("Upgrading workspace content completed.");

            logger.info("Copying version store content");
            copyVersionStore(builder, root, workspaceName, uriToPrefix, versionablePaths);
            logger.debug("Upgrading version store content completed.");

            logger.info("Applying default commit hooks");
            // TODO: default hooks?
            List<CommitHook> hooks = newArrayList();

            UserConfiguration userConf =
                    security.getConfiguration(UserConfiguration.class);
            String groupsPath = userConf.getParameters().getConfigValue(
                    UserConstants.PARAM_GROUP_PATH,
                    UserConstants.DEFAULT_GROUP_PATH);

            // hooks specific to the upgrade, need to run first
            hooks.add(new EditorHook(new CompositeEditorProvider(
                    new RestrictionEditorProvider(),
                    new GroupEditorProvider(groupsPath))));

            // security-related hooks
            for (SecurityConfiguration sc : security.getConfigurations()) {
                hooks.addAll(sc.getCommitHooks(workspaceName));
            }

            if (customCommitHooks != null) {
                hooks.addAll(customCommitHooks);
            }

            // type validation, reference and indexing hooks
            hooks.add(new EditorHook(new CompositeEditorProvider(
                createTypeEditorProvider(),
                createIndexEditorProvider()
            )));

            target.merge(builder, new LoggingCompositeHook(hooks), CommitInfo.EMPTY);
            logger.debug("Repository upgrade completed.");
        } catch (Exception e) {
            throw new RepositoryException("Failed to copy content", e);
        }
    }

    private static EditorProvider createTypeEditorProvider() {
        return new EditorProvider() {
            @Override
            public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info)
                    throws CommitFailedException {
                Editor rootEditor = new TypeEditorProvider(false)
                        .getRootEditor(before, after, builder, info);
                return ProgressNotificationEditor.wrap(rootEditor, logger, "Checking node types:");
            }

            @Override
            public String toString() {
                return "TypeEditorProvider";
            }
        };
    }

    private static EditorProvider createIndexEditorProvider() {
        final ProgressTicker ticker = new AsciiArtTicker();
        return new EditorProvider() {
            @Override
            public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info) {
                IndexEditorProvider editorProviders = new CompositeIndexEditorProvider(
                        new ReferenceEditorProvider(),
                        new PropertyIndexEditorProvider());

                return new IndexUpdate(editorProviders, null, after, builder, new IndexUpdateCallback() {
                    String progress = "Updating indexes ";
                    long t0;
                    @Override
                    public void indexUpdate() {
                        long t = System.currentTimeMillis();
                        if (t - t0 > 2000) {
                            logger.info("{} {}", progress, ticker.tick());
                            t0 = t ;
                        }
                    }
                });
            }

            @Override
            public String toString() {
                return "IndexEditorProvider";
            }
        };
    }

    protected ConfigurationParameters mapSecurityConfig(SecurityConfig config) {
        ConfigurationParameters loginConfig = mapConfigurationParameters(
                config.getLoginModuleConfig(),
                LoginModuleConfig.PARAM_ADMIN_ID, UserConstants.PARAM_ADMIN_ID,
                LoginModuleConfig.PARAM_ANONYMOUS_ID, UserConstants.PARAM_ANONYMOUS_ID);
        ConfigurationParameters userConfig = mapConfigurationParameters(
                config.getSecurityManagerConfig().getUserManagerConfig(),
                UserManagerImpl.PARAM_USERS_PATH, UserConstants.PARAM_USER_PATH,
                UserManagerImpl.PARAM_GROUPS_PATH, UserConstants.PARAM_GROUP_PATH,
                UserManagerImpl.PARAM_DEFAULT_DEPTH, UserConstants.PARAM_DEFAULT_DEPTH,
                UserManagerImpl.PARAM_PASSWORD_HASH_ALGORITHM, UserConstants.PARAM_PASSWORD_HASH_ALGORITHM,
                UserManagerImpl.PARAM_PASSWORD_HASH_ITERATIONS, UserConstants.PARAM_PASSWORD_HASH_ITERATIONS);
        return ConfigurationParameters.of(ImmutableMap.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(loginConfig, userConfig)));
    }

    protected ConfigurationParameters mapConfigurationParameters(
            BeanConfig config, String... mapping) {
        Map<String, String> map = newHashMap();
        if (config != null) {
            Properties properties = config.getParameters();
            for (int i = 0; i + 1 < mapping.length; i += 2) {
                String value = properties.getProperty(mapping[i]);
                if (value != null) {
                    map.put(mapping[i + 1], value);
                }
            }
        }
        return ConfigurationParameters.of(map);
    }

    private String getOakName(Name name) throws NamespaceException {
        String uri = name.getNamespaceURI();
        String local = name.getLocalName();
        if (uri == null || uri.isEmpty()) {
            return local;
        } else {
            return source.getNamespaceRegistry().getPrefix(uri) + ':' + local;
        }
    }

    /**
     * Copies the registered namespaces to the target repository, and returns
     * the internal namespace index mapping used in bundle serialization.
     *
     * @param root root builder
     * @param uriToPrefix namespace URI to prefix mapping
     * @throws RepositoryException
     */
    private void copyNamespaces(
            NodeBuilder root,
            Map<String, String> uriToPrefix)
            throws RepositoryException {
        NodeBuilder system = root.child(JCR_SYSTEM);
        NodeBuilder namespaces = system.child(NamespaceConstants.REP_NAMESPACES);

        Properties registry = loadProperties("/namespaces/ns_reg.properties");
        for (String prefixHint : registry.stringPropertyNames()) {
            String prefix;
            String uri = registry.getProperty(prefixHint);
            if (".empty.key".equals(prefixHint)) {
                prefix = ""; // the default empty mapping is not stored
            } else {
                prefix = addCustomMapping(namespaces, uri, prefixHint);
            }
            checkState(uriToPrefix.put(uri, prefix) == null);
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
    private void copyCustomPrivileges(PrivilegeManager pMgr) throws RepositoryException {
        PrivilegeRegistry registry = source.getPrivilegeRegistry();

        List<Privilege> customAggrPrivs = Lists.newArrayList();

        logger.debug("Registering custom non-aggregated privileges");
        for (Privilege privilege : registry.getRegisteredPrivileges()) {
            String privilegeName = privilege.getName();
            if (PrivilegeBits.BUILT_IN.containsKey(privilegeName) || JCR_ALL.equals(privilegeName)) {
                // Ignore built in privileges as those have been installed by the PrivilegesInitializer already
                logger.debug("Built-in privilege -> ignore.");
            } else if (privilege.isAggregate()) {
                // postpone
                customAggrPrivs.add(privilege);
            } else {
                pMgr.registerPrivilege(privilegeName, privilege.isAbstract(), new String[0]);
                logger.info("- " + privilegeName);
            }
        }

        logger.debug("Registering custom aggregated privileges");
        while (!customAggrPrivs.isEmpty()) {
            Iterator<Privilege> it = customAggrPrivs.iterator();
            boolean progress = false;
            while (it.hasNext()) {
                Privilege aggrPriv = it.next();

                List<String> aggrNames = Lists.transform(
                        ImmutableList.copyOf(aggrPriv.getDeclaredAggregatePrivileges()),
                        new Function<Privilege, String>() {
                            @Nullable
                            @Override
                            public String apply(@Nullable Privilege input) {
                                return (input == null) ? null : input.getName();
                            }
                        });
                if (allAggregatesRegistered(pMgr, aggrNames)) {
                    pMgr.registerPrivilege(aggrPriv.getName(), aggrPriv.isAbstract(), aggrNames.toArray(new String[aggrNames.size()]));
                    it.remove();
                    logger.info("- " + aggrPriv.getName());
                    progress = true;
                }
            }
            if (!progress) {
                break;
            }
        }

        if (customAggrPrivs.isEmpty()) {
            logger.debug("Registration of custom privileges completed.");
        } else {
            StringBuilder invalid = new StringBuilder("|");
            for (Privilege p : customAggrPrivs) {
                invalid.append(p.getName()).append('|');
            }
            throw new RepositoryException("Failed to register custom privileges. The following privileges contained an invalid aggregation:" + invalid);
        }
    }

    private static boolean allAggregatesRegistered(PrivilegeManager privilegeManager, List<String> aggrNames) {
        for (String name : aggrNames) {
            try {
                privilegeManager.getPrivilege(name);
            } catch (RepositoryException e) {
                return false;
            }
        }
        return true;
    }

    private void copyNodeTypes(NodeTypeManager ntMgr, ValueFactory valueFactory) throws RepositoryException {
        NodeTypeRegistry sourceRegistry = source.getNodeTypeRegistry();
        List<NodeTypeTemplate> templates = Lists.newArrayList();
        for (Name name : sourceRegistry.getRegisteredNodeTypes()) {
            String oakName = getOakName(name);
            // skip built-in nodetypes (OAK-1235)
            if (!ntMgr.hasNodeType(oakName)) {
                QNodeTypeDefinition def = sourceRegistry.getNodeTypeDef(name);
                templates.add(createNodeTypeTemplate(valueFactory, ntMgr, oakName, def));
            }
        }
        ntMgr.registerNodeTypes(templates.toArray(new NodeTypeTemplate[templates.size()]), true);
    }

    private NodeTypeTemplate createNodeTypeTemplate(ValueFactory valueFactory, NodeTypeManager ntMgr, String oakName, QNodeTypeDefinition def) throws RepositoryException {
        NodeTypeTemplate tmpl = ntMgr.createNodeTypeTemplate();
        tmpl.setName(oakName);
        tmpl.setAbstract(def.isAbstract());
        tmpl.setMixin(def.isMixin());
        tmpl.setOrderableChildNodes(def.hasOrderableChildNodes());
        tmpl.setQueryable(def.isQueryable());

        Name primaryItemName = def.getPrimaryItemName();
        if (primaryItemName != null) {
            tmpl.setPrimaryItemName(getOakName(primaryItemName));
        }

        Name[] supertypes = def.getSupertypes();
        if (supertypes != null && supertypes.length > 0) {
            List<String> names = newArrayListWithCapacity(supertypes.length);
            for (Name supertype : supertypes) {
                names.add(getOakName(supertype));
            }
            tmpl.setDeclaredSuperTypeNames(names.toArray(new String[names.size()]));
        }

        List<PropertyDefinitionTemplate> propertyDefinitionTemplates = tmpl.getPropertyDefinitionTemplates();
        for (QPropertyDefinition qpd : def.getPropertyDefs()) {
            PropertyDefinitionTemplate pdt = createPropertyDefinitionTemplate(valueFactory, ntMgr, qpd);
            propertyDefinitionTemplates.add(pdt);
        }

        // + jcr:childNodeDefinition (nt:childNodeDefinition) = nt:childNodeDefinition protected sns
        List<NodeDefinitionTemplate> nodeDefinitionTemplates = tmpl.getNodeDefinitionTemplates();
        for (QNodeDefinition qnd : def.getChildNodeDefs()) {
            NodeDefinitionTemplate ndt = createNodeDefinitionTemplate(ntMgr, qnd);
            nodeDefinitionTemplates.add(ndt);
        }

        return tmpl;
    }

    private NodeDefinitionTemplate createNodeDefinitionTemplate(NodeTypeManager ntMgr, QNodeDefinition def) throws RepositoryException {
        NodeDefinitionTemplate tmpl = ntMgr.createNodeDefinitionTemplate();

        Name name = def.getName();
        if (name != null && !name.equals(ANY_NAME)) {
            tmpl.setName(getOakName(name));
        }
        tmpl.setAutoCreated(def.isAutoCreated());
        tmpl.setMandatory(def.isMandatory());
        tmpl.setOnParentVersion(def.getOnParentVersion());
        tmpl.setProtected(def.isProtected());
        tmpl.setSameNameSiblings(def.allowsSameNameSiblings());

        List<String> names = newArrayListWithCapacity(def.getRequiredPrimaryTypes().length);
        for (Name type : def.getRequiredPrimaryTypes()) {
            names.add(getOakName(type));
        }
        tmpl.setRequiredPrimaryTypeNames(names.toArray(new String[names.size()]));

        Name type = def.getDefaultPrimaryType();
        if (type != null) {
            tmpl.setDefaultPrimaryTypeName(getOakName(type));
        }

        return tmpl;
    }

    private PropertyDefinitionTemplate createPropertyDefinitionTemplate(ValueFactory valueFactory, NodeTypeManager ntMgr, QPropertyDefinition def) throws RepositoryException {
        PropertyDefinitionTemplate tmpl = ntMgr.createPropertyDefinitionTemplate();

        Name name = def.getName();
        if (name != null && !name.equals(ANY_NAME)) {
            tmpl.setName(getOakName(name));
        }
        tmpl.setAutoCreated(def.isAutoCreated());
        tmpl.setMandatory(def.isMandatory());
        tmpl.setOnParentVersion(def.getOnParentVersion());
        tmpl.setProtected(def.isProtected());
        tmpl.setRequiredType(def.getRequiredType());
        tmpl.setMultiple(def.isMultiple());
        tmpl.setAvailableQueryOperators(def.getAvailableQueryOperators());
        tmpl.setFullTextSearchable(def.isFullTextSearchable());
        tmpl.setQueryOrderable(def.isQueryOrderable());

        QValueConstraint[] qConstraints = def.getValueConstraints();
        if (qConstraints != null && qConstraints.length > 0) {
            String[] constraints = new String[qConstraints.length];
            for (int i = 0; i < qConstraints.length; i++) {
                constraints[i] = qConstraints[i].getString();
            }
            tmpl.setValueConstraints(constraints);
        }

        QValue[] qValues = def.getDefaultValues();
        if (qValues != null) {
            NamePathResolver npResolver = new DefaultNamePathResolver(source.getNamespaceRegistry());
            Value[] vs = new Value[qValues.length];
            for (int i = 0; i < qValues.length; i++) {
                vs[i] = ValueFormat.getJCRValue(qValues[i], npResolver, valueFactory);
            }
            tmpl.setDefaultValues(vs);
        }

        return tmpl;
    }

    private void copyVersionStore(
            NodeBuilder builder, NodeState root, String workspaceName,
            Map<String, String> uriToPrefix,
            Map<String, String> versionablePaths) {
        PersistenceManager pm = source.getInternalVersionManager().getPersistenceManager();
        NodeBuilder system = builder.child(JCR_SYSTEM);

        logger.info("Copying version histories");
        copyState(system, JCR_VERSIONSTORAGE, new JackrabbitNodeState(
                pm, root, uriToPrefix, VERSION_STORAGE_NODE_ID,
                "/jcr:system/jcr:versionStorage",
                workspaceName, versionablePaths, copyBinariesByReference));

        logger.info("Copying activities");
        copyState(system, "jcr:activities", new JackrabbitNodeState(
                pm, root, uriToPrefix, ACTIVITIES_NODE_ID,
                "/jcr:system/jcr:activities",
                workspaceName, versionablePaths, copyBinariesByReference));
    }

    private String copyWorkspace(
            NodeBuilder builder, NodeState root, String workspaceName,
            Map<String, String> uriToPrefix, Map<String, String> versionablePaths)
            throws RepositoryException {
        logger.info("Copying workspace {}", workspaceName);

        PersistenceManager pm =
                source.getWorkspaceInfo(workspaceName).getPersistenceManager();

        NodeState state = new JackrabbitNodeState(
                pm, root, uriToPrefix, ROOT_NODE_ID, "/",
                workspaceName, versionablePaths, copyBinariesByReference);

        for (PropertyState property : state.getProperties()) {
            builder.setProperty(property);
        }
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            String childName = child.getName();
            if (!JCR_SYSTEM.equals(childName)) {
                logger.info("Copying subtree /{}", childName);
                copyState(builder, childName, child.getNodeState());
            }
        }

        return workspaceName;
    }

    private void copyState(NodeBuilder parent, String name, NodeState state) {
        if (parent instanceof SegmentNodeBuilder) {
            parent.setChildNode(name, state);
        } else {
            setChildNode(parent, name, state);
        }
    }

    /**
     * NodeState are copied by value by recursing down the complete tree
     * This is a temporary approach for OAK-1760 for 1.0 branch.
     */
    private void setChildNode(NodeBuilder parent, String name, NodeState state) {
        // OAK-1589: maximum supported length of name for DocumentNodeStore
        // is 150 bytes. Skip the sub tree if the the name is too long
        if (name.length() > 37 && name.getBytes(Charsets.UTF_8).length > 150) {
            logger.warn("Node name too long. Skipping {}", state);
            return;
        }
        NodeBuilder builder = parent.setChildNode(name);
        for (PropertyState property : state.getProperties()) {
            builder.setProperty(property);
        }
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            setChildNode(builder, child.getName(), child.getNodeState());
        }
    }

    private static class LoggingCompositeHook implements CommitHook {
        private final Collection<CommitHook> hooks;

        public LoggingCompositeHook(Collection<CommitHook> hooks) {
            this.hooks = hooks;
        }

        @Nonnull
        @Override
        public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
            NodeState newState = after;
            Stopwatch watch = Stopwatch.createStarted();
            for (CommitHook hook : hooks) {
                logger.info("Processing commit via {}", hook);
                newState = hook.processCommit(before, newState, info);
                logger.info("Commit hook {} processed commit in {}", hook, watch);
                watch.reset().start();
            }
            return newState;
        }
    }
}
