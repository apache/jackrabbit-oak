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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.migration.FilteringNodeState.ALL;
import static org.apache.jackrabbit.oak.plugins.migration.FilteringNodeState.NONE;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier.copyProperties;
import static org.apache.jackrabbit.oak.plugins.name.Namespaces.addCustomMapping;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory.SKIP_NAME_CHECK;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.NamespaceException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.core.IndexAccessor;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.BeanConfig;
import org.apache.jackrabbit.core.config.LoginModuleConfig;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.core.config.SecurityConfig;
import org.apache.jackrabbit.core.fs.FileSystem;
import org.apache.jackrabbit.core.fs.FileSystemException;
import org.apache.jackrabbit.core.nodetype.NodeTypeRegistry;
import org.apache.jackrabbit.core.query.lucene.FieldNames;
import org.apache.jackrabbit.core.security.authorization.PrivilegeRegistry;
import org.apache.jackrabbit.core.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.migration.NameFilteringNodeState;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier;
import org.apache.jackrabbit.oak.plugins.migration.report.LoggingReporter;
import org.apache.jackrabbit.oak.plugins.migration.report.ReportingNodeState;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.security.AuthorizableFolderEditor;
import org.apache.jackrabbit.oak.upgrade.security.GroupEditorProvider;
import org.apache.jackrabbit.oak.upgrade.security.RestrictionEditorProvider;
import org.apache.jackrabbit.oak.upgrade.version.VersionCopyConfiguration;
import org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil;
import org.apache.jackrabbit.oak.upgrade.version.VersionableEditor;
import org.apache.jackrabbit.oak.upgrade.version.VersionablePropertiesEditor;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.QNodeDefinition;
import org.apache.jackrabbit.spi.QNodeTypeDefinition;
import org.apache.jackrabbit.spi.QPropertyDefinition;
import org.apache.jackrabbit.spi.QValue;
import org.apache.jackrabbit.spi.QValueConstraint;
import org.apache.jackrabbit.spi.commons.conversion.DefaultNamePathResolver;
import org.apache.jackrabbit.spi.commons.conversion.NamePathResolver;
import org.apache.jackrabbit.spi.commons.value.ValueFormat;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.upgrade.version.VersionCopier.copyVersionStorage;
import static org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil.getVersionStorage;

public class RepositoryUpgrade {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryUpgrade.class);

    private static final int LOG_NODE_COPY = Integer.getInteger("oak.upgrade.logNodeCopy", 10000);

    public static final Set<String> DEFAULT_INCLUDE_PATHS = ALL;

    public static final Set<String> DEFAULT_EXCLUDE_PATHS = NONE;

    public static final Set<String> DEFAULT_FRAGMENT_PATHS = NONE;

    public static final Set<String> DEFAULT_EXCLUDE_FRAGMENTS = NONE;

    public static final Set<String> DEFAULT_MERGE_PATHS = NONE;

    /**
     * Source repository context.
     */
    private final RepositoryContext source;

    /**
     * Target node store.
     */
    private final NodeStore target;

    /**
     * Paths to include during the copy process. Defaults to the root path "/".
     */
    private Set<String> includePaths = DEFAULT_INCLUDE_PATHS;

    /**
     * Paths to exclude during the copy process. Empty by default.
     */
    private Set<String> excludePaths = DEFAULT_EXCLUDE_PATHS;

    /**
     * Paths supporting fragments during the copy process. Empty by default.
     */
    private Set<String> fragmentPaths = DEFAULT_FRAGMENT_PATHS;

    /**
     * Fragments to exclude during the copy process. Empty by default.
     */
    private Set<String> excludeFragments = DEFAULT_EXCLUDE_FRAGMENTS;

    /**
     * Paths to merge during the copy process. Empty by default.
     */
    private Set<String> mergePaths = DEFAULT_MERGE_PATHS;

    /**
     * Whether or not to copy binaries by reference. Defaults to false.
     */
    private boolean copyBinariesByReference = false;

    private boolean skipOnError = false;

    private boolean earlyShutdown = false;

    private List<CommitHook> customCommitHooks = null;

    private boolean checkLongNames = false;

    private boolean filterLongNames = true;

    private boolean skipInitialization = false;

    VersionCopyConfiguration versionCopyConfiguration = new VersionCopyConfiguration();

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

    public boolean isSkipOnError() {
        return skipOnError;
    }

    public void setSkipOnError(boolean skipOnError) {
        this.skipOnError = skipOnError;
    }

    public boolean isEarlyShutdown() {
        return earlyShutdown;
    }

    public void setEarlyShutdown(boolean earlyShutdown) {
        this.earlyShutdown = earlyShutdown;
    }

    public boolean isCheckLongNames() {
        return checkLongNames;
    }

    public void setCheckLongNames(boolean checkLongNames) {
        this.checkLongNames = checkLongNames;
    }

    public boolean isFilterLongNames() {
        return filterLongNames;
    }

    public void setFilterLongNames(boolean filterLongNames) {
        this.filterLongNames = filterLongNames;
    }

    public boolean isSkipInitialization() {
        return skipInitialization;
    }

    public void setSkipInitialization(boolean skipInitialization) {
        this.skipInitialization = skipInitialization;
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
     * Sets the paths that should be included when the source repository
     * is copied to the target repository.
     *
     * @param includes Paths to be included in the copy.
     */
    public void setIncludes(@Nonnull String... includes) {
        this.includePaths = copyOf(checkNotNull(includes));
    }

    /**
     * Sets the paths that should be excluded when the source repository
     * is copied to the target repository.
     *
     * @param excludes Paths to be excluded from the copy.
     */
    public void setExcludes(@Nonnull String... excludes) {
        this.excludePaths = copyOf(checkNotNull(excludes));
    }

    /**
     * Sets the paths that should support the fragments.
     *
     * @param fragmentPaths Paths that should support fragments.
     */
    public void setFragmentPaths(@Nonnull String... fragmentPaths) {
        this.fragmentPaths = copyOf(checkNotNull(fragmentPaths));
    }

    /**
     * Sets the name fragments that should be excluded when the source repository
     * is copied to the target repository.
     *
     * @param excludes Name fragments to be excluded from the copy.
     */
    public void setExcludeFragments(@Nonnull String... excludes) {
        this.excludeFragments = copyOf(checkNotNull(excludes));
    }

    /**
     * Sets the paths that should be merged when the source repository
     * is copied to the target repository.
     *
     * @param merges Paths to be merged during copy.
     */
    public void setMerges(@Nonnull String... merges) {
        this.mergePaths = copyOf(checkNotNull(merges));
    }

    /**
     * Configures the version storage copy. Be default all versions are copied.
     * One may disable it completely by setting {@code null} here or limit it to
     * a selected date range: {@code <minDate, now()>}.
     * 
     * @param minDate
     *            minimum date of the versions to copy or {@code null} to
     *            disable the storage version copying completely. Default value:
     *            {@code 1970-01-01 00:00:00}.
     */
    public void setCopyVersions(Calendar minDate) {
        versionCopyConfiguration.setCopyVersions(minDate);
    }

    /**
     * Configures copying of the orphaned version histories (eg. ones that are
     * not referenced by the existing nodes). By default all orphaned version
     * histories are copied. One may disable it completely by setting
     * {@code null} here or limit it to a selected date range:
     * {@code <minDate, now()>}. <br>
     * <br>
     * Please notice, that this option is overriden by the
     * {@link #setCopyVersions(Calendar)}. You can't copy orphaned versions
     * older than set in {@link #setCopyVersions(Calendar)} and if you set
     * {@code null} there, this option will be ignored.
     * 
     * @param minDate
     *            minimum date of the orphaned versions to copy or {@code null}
     *            to not copy them at all. Default value:
     *            {@code 1970-01-01 00:00:00}.
     */
    public void setCopyOrphanedVersions(Calendar minDate) {
        versionCopyConfiguration.setCopyOrphanedVersions(minDate);
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
        if (checkLongNames) {
            assertNoLongNames();
        }

        RepositoryConfig config = source.getRepositoryConfig();
        logger.info("Copying repository content from {} to Oak", config.getHomeDir());
        try {
            NodeBuilder targetBuilder = target.getRoot().builder();
            if (VersionHistoryUtil.getVersionStorage(targetBuilder).exists() && !versionCopyConfiguration.skipOrphanedVersionsCopy()) {
                logger.warn("The version storage on destination already exists. Orphaned version histories will be skipped.");
                versionCopyConfiguration.setCopyOrphanedVersions(null);
            }
            final Root upgradeRoot = new UpgradeRoot(targetBuilder);

            String workspaceName =
                    source.getRepositoryConfig().getDefaultWorkspaceName();
            SecurityProviderImpl security = new SecurityProviderImpl(
                    mapSecurityConfig(config.getSecurityConfig()));

            if (skipInitialization) {
                logger.info("Skipping the repository initialization");
            } else {
                // init target repository first
                logger.info("Initializing initial repository content from {}", config.getHomeDir());
                new InitialContent().initialize(targetBuilder);
                if (initializer != null) {
                    initializer.initialize(targetBuilder);
                }
                logger.debug("InitialContent completed from {}", config.getHomeDir());

                for (SecurityConfiguration sc : security.getConfigurations()) {
                    RepositoryInitializer ri = sc.getRepositoryInitializer();
                    ri.initialize(targetBuilder);
                    logger.debug("Repository initializer '" + ri.getClass().getName() + "' completed", config.getHomeDir());
                }
                for (SecurityConfiguration sc : security.getConfigurations()) {
                    WorkspaceInitializer wi = sc.getWorkspaceInitializer();
                    wi.initialize(targetBuilder, workspaceName);
                    logger.debug("Workspace initializer '" + wi.getClass().getName() + "' completed", config.getHomeDir());
                }
            }

            HashBiMap<String, String> uriToPrefix = HashBiMap.create();
            logger.info("Copying registered namespaces");
            copyNamespaces(targetBuilder, uriToPrefix);
            logger.debug("Namespace registration completed.");

            if (skipInitialization) {
                logger.info("Skipping registering node types and privileges");
            } else {
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
                        targetBuilder.getBaseState(), targetBuilder.getNodeState(), targetBuilder, null);
            }

            final NodeState reportingSourceRoot = ReportingNodeState.wrap(
                    JackrabbitNodeState.createRootNodeState(
                            source, workspaceName, targetBuilder.getNodeState(), 
                            uriToPrefix, copyBinariesByReference, skipOnError
                    ),
                    new LoggingReporter(logger, "Migrating", LOG_NODE_COPY, -1)
            );
            final NodeState sourceRoot;
            if (filterLongNames) {
                sourceRoot = NameFilteringNodeState.wrap(reportingSourceRoot);
            } else {
                sourceRoot = reportingSourceRoot;
            }

            final Stopwatch watch = Stopwatch.createStarted();

            logger.info("Copying workspace content");
            copyWorkspace(sourceRoot, targetBuilder, workspaceName);
            targetBuilder.getNodeState(); // on TarMK this does call triggers the actual copy
            logger.info("Upgrading workspace content completed in {}s ({})", watch.elapsed(TimeUnit.SECONDS), watch);

            if (!versionCopyConfiguration.skipOrphanedVersionsCopy()) {
                logger.info("Copying version storage");
                watch.reset().start();
                copyVersionStorage(targetBuilder, getVersionStorage(sourceRoot), getVersionStorage(targetBuilder), versionCopyConfiguration);
                targetBuilder.getNodeState(); // on TarMK this does call triggers the actual copy
                logger.info("Version storage copied in {}s ({})", watch.elapsed(TimeUnit.SECONDS), watch);
            } else {
                logger.info("Skipping the version storage as the copyOrphanedVersions is set to false");
            }

            watch.reset().start();
            logger.info("Applying default commit hooks");
            // TODO: default hooks?
            List<CommitHook> hooks = newArrayList();

            UserConfiguration userConf =
                    security.getConfiguration(UserConfiguration.class);
            String groupsPath = userConf.getParameters().getConfigValue(
                    UserConstants.PARAM_GROUP_PATH,
                    UserConstants.DEFAULT_GROUP_PATH);
            String usersPath = userConf.getParameters().getConfigValue(
                    UserConstants.PARAM_USER_PATH,
                    UserConstants.DEFAULT_USER_PATH);

            // hooks specific to the upgrade, need to run first
            hooks.add(new EditorHook(new CompositeEditorProvider(
                    new RestrictionEditorProvider(),
                    new GroupEditorProvider(groupsPath),
                    // copy referenced version histories
                    new VersionableEditor.Provider(sourceRoot, workspaceName, versionCopyConfiguration),
                    new SameNameSiblingsEditor.Provider(),
                    AuthorizableFolderEditor.provider(groupsPath, usersPath)
            )));

            // this editor works on the VersionableEditor output, so it can't be
            // a part of the same EditorHook
            hooks.add(new EditorHook(new VersionablePropertiesEditor.Provider()));

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

            target.merge(targetBuilder, new LoggingCompositeHook(hooks, source, overrideEarlyShutdown()), CommitInfo.EMPTY);
            logger.info("Processing commit hooks completed in {}s ({})", watch.elapsed(TimeUnit.SECONDS), watch);
            logger.debug("Repository upgrade completed.");
        } catch (Exception e) {
            throw new RepositoryException("Failed to copy content", e);
        }
    }

    private boolean overrideEarlyShutdown() {
        if (earlyShutdown == false) {
            return false;
        }

        final VersionCopyConfiguration c = this.versionCopyConfiguration;
        if (c.isCopyVersions() && c.skipOrphanedVersionsCopy()) {
            logger.info("Overriding early shutdown to false because of the copy versions settings");
            return false;
        }
        if (c.isCopyVersions() && !c.skipOrphanedVersionsCopy()
                && c.getOrphanedMinDate().after(c.getVersionsMinDate())) {
            logger.info("Overriding early shutdown to false because of the copy versions settings");
            return false;
        }
        return true;
    }

    static EditorProvider createTypeEditorProvider() {
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

    static EditorProvider createIndexEditorProvider() {
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
        ConfigurationParameters userConfig;
        if (config.getSecurityManagerConfig() == null) {
            userConfig = ConfigurationParameters.EMPTY;
        } else {
            userConfig = mapConfigurationParameters(
                    config.getSecurityManagerConfig().getUserManagerConfig(),
                    UserManagerImpl.PARAM_USERS_PATH, UserConstants.PARAM_USER_PATH,
                    UserManagerImpl.PARAM_GROUPS_PATH, UserConstants.PARAM_GROUP_PATH,
                    UserManagerImpl.PARAM_DEFAULT_DEPTH, UserConstants.PARAM_DEFAULT_DEPTH,
                    UserManagerImpl.PARAM_PASSWORD_HASH_ALGORITHM, UserConstants.PARAM_PASSWORD_HASH_ALGORITHM,
                    UserManagerImpl.PARAM_PASSWORD_HASH_ITERATIONS, UserConstants.PARAM_PASSWORD_HASH_ITERATIONS);
        }
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
     * @param targetRoot root builder of the target store
     * @param uriToPrefix namespace URI to prefix mapping
     * @throws RepositoryException
     */
    private void copyNamespaces(
            NodeBuilder targetRoot,
            Map<String, String> uriToPrefix)
            throws RepositoryException {
        NodeBuilder system = targetRoot.child(JCR_SYSTEM);
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

            if (hasPrivilege(pMgr, privilegeName)) {
                logger.debug("Privilege {} already exists", privilegeName);
                continue;
            }

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

    private boolean hasPrivilege(PrivilegeManager pMgr, String privilegeName) throws RepositoryException {
        final Privilege[] registeredPrivileges = pMgr.getRegisteredPrivileges();
        for (Privilege registeredPrivilege : registeredPrivileges) {
            if (registeredPrivilege.getName().equals(privilegeName)) {
                return true;
            }
        }
        return false;
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
        if (name != null) {
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
        if (name != null) {
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

    private String copyWorkspace(NodeState sourceRoot, NodeBuilder targetRoot, String workspaceName)
            throws RepositoryException {
        final Set<String> includes = calculateEffectiveIncludePaths(includePaths, sourceRoot);
        final Set<String> excludes = union(copyOf(this.excludePaths), of("/jcr:system/jcr:versionStorage"));
        final Set<String> merges = union(copyOf(this.mergePaths), of("/jcr:system"));

        logger.info("Copying workspace {} [i: {}, e: {}, m: {}]", workspaceName, includes, excludes, merges);

        NodeStateCopier.builder()
                .include(includes)
                .exclude(excludes)
                .supportFragment(fragmentPaths)
                .excludeFragments(excludeFragments)
                .merge(merges)
                .copy(sourceRoot, targetRoot);

        if (includePaths.contains("/")) {
            copyProperties(sourceRoot, targetRoot);
        }

        return workspaceName;
    }

    static Set<String> calculateEffectiveIncludePaths(Set<String> includePaths, NodeState sourceRoot) {
        if (!includePaths.contains("/")) {
            return copyOf(includePaths);
        }

        // include child nodes from source individually to avoid deleting other initialized content
        final Set<String> includes = newHashSet();
        for (String childNodeName : sourceRoot.getChildNodeNames()) {
            includes.add("/" + childNodeName);
        }
        return includes;
    }

    void assertNoLongNames() throws RepositoryException {
        Session session = source.getRepository().login(null, null);
        boolean longNameFound = false;
        try {
            IndexReader reader = IndexAccessor.getReader(source);
            if (reader == null) {
                return;
            }
            TermEnum terms = reader.terms(new Term(FieldNames.LOCAL_NAME));
            while (terms.next()) {
                Term t = terms.term();
                if (!FieldNames.LOCAL_NAME.equals(t.field())) {
                    continue;
                }
                String name = t.text();
                if (NameFilteringNodeState.isNameTooLong(name)) {
                    TermDocs docs = reader.termDocs(t);
                    if (docs.next()) {
                        int docId = docs.doc();
                        String uuid = reader.document(docId).get(FieldNames.UUID);
                        Node n = session.getNodeByIdentifier(uuid);
                        logger.warn("Name too long: {}", n.getPath());
                        longNameFound = true;
                    }
                }
            }
        } catch (IOException e) {
            throw new RepositoryException(e);
        } finally {
            session.logout();
        }
        if (longNameFound) {
            logger.error("Node with a long name has been found. Please fix the content or rerun the migration with {} option.", SKIP_NAME_CHECK);
            throw new RepositoryException("Node with a long name has been found.");
        }
    }

    static class LoggingCompositeHook implements CommitHook {
        private final Collection<CommitHook> hooks;
        private boolean started = false;
        private final boolean earlyShutdown;
        private final RepositoryContext source;

        public LoggingCompositeHook(Collection<CommitHook> hooks,
                  RepositoryContext source, boolean earlyShutdown) {
            this.hooks = hooks;
            this.earlyShutdown = earlyShutdown;
            this.source = source;
        }

        @Nonnull
        @Override
        public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
            NodeState newState = after;
            Stopwatch watch = Stopwatch.createStarted();
            if (earlyShutdown && source != null && !started) {
                logger.info("Shutting down source repository.");
                source.getRepository().shutdown();
                started = true;
            }
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
