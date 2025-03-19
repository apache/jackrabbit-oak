/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.composite.blueGreen;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.composite.CompositeNodeStore;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.WhiteboardIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.observation.ChangeCollectorProvider;
import org.apache.jackrabbit.oak.plugins.version.VersionHook;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.WhiteboardIndexProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;

import org.jetbrains.annotations.Nullable;

/**
 * Utility class to open a repository.
 */
public class Persistence {

    private static final MountInfoProvider MOUNT_INFO_PROVIDER = createMountInfoProvider();

    private final ArrayList<FileStore> fileStores = new ArrayList<>();
    private JackrabbitRepository repository;
    private NodeStore compositeNodestore;

    public Session session;

    public static Persistence open(File directory, Config config) throws Exception {
        Persistence result = new Persistence();
        FileStore fs = openFileStore(directory, config.blobStore);
        result.fileStores.add(fs);
        SegmentNodeStore ns = openSegmentStore(fs);
        result.repository = openRepsitory(ns, config.indexDir);
        result.session = result.repository.login(createCredentials());
        return result;
    }

    public static Persistence openComposite(File globalDir, File libsDir, Config config) throws Exception {
        Persistence result = new Persistence();
        FileStore libsFileStore = openFileStore(libsDir, config.blobStore);
        result.fileStores.add(libsFileStore);
        SegmentNodeStore libsStore = openSegmentStore(libsFileStore);
        FileStore globalFileStore = openFileStore(globalDir, config.blobStore);
        result.fileStores.add(globalFileStore);
        SegmentNodeStore globalStore = openSegmentStore(globalFileStore);
        result.compositeNodestore = new CompositeNodeStore.Builder(
                MOUNT_INFO_PROVIDER,
                globalStore).addMount("libs", libsStore).
                setPartialReadOnly(true).build();
        result.repository = openRepsitory(result.compositeNodestore, config.indexDir);
        result.session = result.repository.login(createCredentials());
        return result;
    }

    @Nullable
    public NodeStore getCompositeNodestore() {
        if (compositeNodestore == null) {
            throw new IllegalStateException("persistence object was not opened in composite mode");
        }
        return compositeNodestore;
    }

    public void close() {
        session.logout();
        repository.shutdown();
        for(FileStore fs : fileStores) {
            fs.close();
        }
    }
    
    private static MountInfoProvider createMountInfoProvider() {
        return Mounts.newBuilder()
                .mount("libs", true, Arrays.asList(
                        // pathsSupportingFragments
                        "/oak:index/*$" 
                ), Arrays.asList(
                        // mountedPaths
                        "/libs",       
                        "/apps",
                        "/jcr:system/rep:permissionStore/oak:mount-libs-crx.default"))
                .build();
    }
    
    public static BlobStore getFileBlobStore(File directory) throws IOException {
        return new FileBlobStore(directory.getAbsolutePath());
    }
    
    private static SecurityProvider createSecurityProvider() {
        Map<String, Object> userConfigMap = new HashMap<>();
        userConfigMap.put(UserConstants.PARAM_GROUP_PATH, "/home/groups");
        userConfigMap.put(UserConstants.PARAM_USER_PATH, "/home/users");
        userConfigMap.put(UserConstants.PARAM_DEFAULT_DEPTH, 1);
        ConfigurationParameters userConfig = ConfigurationParameters.of(Map.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(userConfigMap)));
        SecurityProvider securityProvider = SecurityProviderBuilder.newBuilder().with(userConfig).build();
        AuthorizationConfiguration acConfig = securityProvider.getConfiguration(AuthorizationConfiguration.class);
        ((AuthorizationConfigurationImpl) ((CompositeAuthorizationConfiguration) acConfig).getDefaultConfig()).bindMountInfoProvider(MOUNT_INFO_PROVIDER);
        return securityProvider;
    }
    
    private static FileStore openFileStore(File directory, BlobStore blobStore) throws IOException {
        try {
            return FileStoreBuilder
                    .fileStoreBuilder(directory)
                    .withBlobStore(blobStore)
                    .build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IOException(e);
        }
    }
    
    private static SegmentNodeStore openSegmentStore(FileStore fileStore) throws IOException {
        return SegmentNodeStoreBuilders
                .builder(fileStore)
                .build();
    }    

    private static JackrabbitRepository openRepsitory(NodeStore nodeStore, File indexDirectory) throws RepositoryException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        Oak oak = new Oak(nodeStore);
        oak.withFailOnMissingIndexProvider();
        InitialContent initialContent = new InitialContent();
        EditorProvider atomicCounter = new AtomicCounterEditorProvider();
        SecurityProvider securityProvider = createSecurityProvider();
        Jcr jcr = new Jcr(oak, false).
                with(new Executor() {
                        @Override
                        public void execute(Runnable command) {
                            executorService.execute(command);
                        }
                    }).
                // with(whiteboard)
                with(initialContent).
                with(new Content()).
                with(JcrConflictHandler.createJcrConflictHandler()).
                with(new VersionHook()).
                with(securityProvider).
                with(new NameValidatorProvider()).
                with(new NamespaceEditorProvider()).
                with(new TypeEditorProvider()).
                with(new ConflictValidatorProvider()).
                with(atomicCounter).
                // one second delay
                withAsyncIndexing("async", 1).
                withAsyncIndexing("fulltext-async", 1);
        ChangeCollectorProvider changeCollectorProvider = new ChangeCollectorProvider();
        jcr.with(changeCollectorProvider);
        
        WhiteboardIndexProvider indexProvider = new WhiteboardIndexProvider();
        WhiteboardIndexEditorProvider indexEditorProvider = new WhiteboardIndexEditorProvider();
        boolean fastQueryResultSize = false;
        jcr.with(indexProvider).
            with(indexEditorProvider).
            with("crx.default").
            withFastQueryResultSize(fastQueryResultSize);
        IndexTracker indexTracker;
        IndexCopier indexCopier;
        try {
            indexCopier = new IndexCopier(executorService, indexDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MountInfoProvider mip = MOUNT_INFO_PROVIDER;
        DefaultIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(
                mip, indexCopier);
        indexTracker = new IndexTracker(indexReaderFactory);
        LuceneIndexProvider luceneIndexProvider =
                new LuceneIndexProvider(indexTracker);
        LuceneIndexEditorProvider luceneIndexEditor =
                new LuceneIndexEditorProvider(indexCopier, indexTracker, null, null, mip);
        jcr.with(new PropertyIndexEditorProvider().with(mip)).
            with(new NodeCounterEditorProvider().with(mip)).
            with(new PropertyIndexProvider().with(mip)).
            with(luceneIndexEditor).
            with((QueryIndexProvider) luceneIndexProvider).
            with((Observer) luceneIndexProvider).
            with(new NodeTypeIndexProvider().with(mip)).
            with(new ReferenceEditorProvider().with(mip)).
            with(new ReferenceIndexProvider().
            with(mip)).
            with(BundlingConfigInitializer.INSTANCE);
        ContentRepository contentRepository = jcr.createContentRepository();
        setupPermissions(contentRepository, securityProvider);
        JackrabbitRepository repository = (JackrabbitRepository) jcr.createRepository();
        return repository;
    }

    private static void setupPermissions(ContentRepository repo,
                                         SecurityProvider securityProvider) throws RepositoryException {
        ContentSession cs = null;
        try {
            cs = Java23Subject.doAsPrivileged(SystemSubject.INSTANCE, new PrivilegedExceptionAction<ContentSession>() {
                @Override
                public ContentSession run() throws Exception {
                    return repo.login(null, null);
                }
            }, null);
    
            Root root = cs.getLatestRoot();
            AuthorizationConfiguration config = securityProvider.getConfiguration(AuthorizationConfiguration.class);
            AccessControlManager acMgr = config.getAccessControlManager(root, NamePathMapper.DEFAULT);
            // protect /oak:index
            setupPolicy("/" + IndexConstants.INDEX_DEFINITIONS_NAME, acMgr);
            // protect /jcr:system
            setupPolicy("/" + JcrConstants.JCR_SYSTEM, acMgr);
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } catch (PrivilegedActionException | CommitFailedException e) {
            throw new RepositoryException(e);
        } finally {
            if (cs != null) {
                try {
                    cs.close();
                } catch (IOException e) {
                    throw new RepositoryException(e);
                }
            }
        }
    }

    private static void setupPolicy(String path, AccessControlManager acMgr) throws RepositoryException {
        // only retrieve applicable policies thus leaving the setup untouched once
        // it has been created and has potentially been modified on an existing
        // instance
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(path);
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            if (policy instanceof JackrabbitAccessControlList) {
                JackrabbitAccessControlList acl = (JackrabbitAccessControlList) policy;
                Privilege[] jcrAll = AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_ALL);
                acl.addEntry(EveryonePrincipal.getInstance(), jcrAll, false);
                acMgr.setPolicy(path, acl);
                break;
            }
        }
    }
    
    private static SimpleCredentials createCredentials() {
        return new SimpleCredentials("admin", "admin".toCharArray());
    }

    private static class Content implements RepositoryInitializer {
        
        private static final String FULLTEXT_ASYNC = "fulltext-async";
        private NodeBuilder index;
        
        @Override
        public void initialize(@NotNull NodeBuilder builder) {
            if (builder.hasChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)) {
                index = builder.child(IndexConstants.INDEX_DEFINITIONS_NAME);
                configureGlobalFullTextIndex();
            }
        }

        private void configureGlobalFullTextIndex() {
            String indexName = "lucene";
            if (!index.hasChildNode(indexName)) {
                Set<String> INCLUDE_PROPS = Set.of("test");
                IndexDefinitionBuilder indexBuilder = new LuceneIndexDefinitionBuilder(index.child(indexName))
                        .codec("Lucene46")
                        .excludedPaths("/libs");
                indexBuilder.async(FULLTEXT_ASYNC, IndexConstants.INDEXING_MODE_NRT);
                indexBuilder.aggregateRule("nt:file", "jcr:content");
                indexBuilder.indexRule("rep:Token");
                LuceneIndexDefinitionBuilder.IndexRule indexRules = indexBuilder.indexRule("nt:base");
                indexRules.includePropertyTypes("String", "Binary");
                for (String includeProp : INCLUDE_PROPS) {
                    indexRules.property(includeProp).propertyIndex();
                }
                indexBuilder.build();
            }
        }        
        
    }

    public MountInfoProvider getMountInfoProvider() {
        return MOUNT_INFO_PROVIDER;
    }

    public static class Config {
        public BlobStore blobStore;
        public File indexDir;
    }

}

