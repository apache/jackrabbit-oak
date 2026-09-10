/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.commons.internal.concurrent.ExecutorHelper;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.LuceneNgIndexCopier;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * OSGi service that provides Lucene 9 index providers.
 * This service registers both the QueryIndexProvider and IndexEditorProvider
 * for handling indexes with type "lucene9".
 */
@Component
@Designate(ocd = LuceneNgIndexProviderService.Config.class)
public class LuceneNgIndexProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexProviderService.class);

    private static final String REPOSITORY_HOME = "repository.home";
    private static final int INDEX_COPIER_POOL_SIZE = 5;

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak LuceneNgIndexProvider",
            description = "Lucene 9 index provider for Oak"
    )
    public @interface Config {
        @AttributeDefinition(
                name = "Disable this component",
                description = "If true, this component is disabled."
        )
        boolean disabled() default false;

        @AttributeDefinition(
                name = "Enable CopyOnRead support",
                description = "Enable copying of Lucene 9 index files to local disk before serving reads. " +
                        "Recommended when the NodeStore's blob store is remote (e.g. S3, Azure), to avoid " +
                        "reading segment files over the network on every query and to keep readiness-probe " +
                        "latency bounded."
        )
        boolean enableCopyOnReadSupport() default true;

        @AttributeDefinition(
                name = "Local index storage path",
                description = "Local file system path where Lucene 9 index files are copied when CopyOnRead " +
                        "is enabled. If not specified, indexes are stored under an 'index' directory under " +
                        "repository home."
        )
        String localIndexDir();

        @AttributeDefinition(
                name = "Prefetch index files",
                description = "When CopyOnRead is enabled, copy all new index files locally before the index " +
                        "is made available to the query engine, instead of copying lazily on first read."
        )
        boolean prefetchIndexFiles() default false;
    }

    private final List<ServiceRegistration<?>> regs = new ArrayList<>();
    private LuceneNgIndexTracker indexTracker;
    private LuceneNgIndexEditorProvider editorProvider;
    private LuceneNgIndexCopier indexCopier;
    private ExecutorService executorService;

    @Activate
    private void activate(BundleContext bundleContext, Config config) {
        if (config.disabled()) {
            LOG.info("LuceneNg component disabled by configuration");
            return;
        }

        LOG.info("Activating LuceneNg Index Provider");

        LuceneNgIndexCopier copier = null;
        if (config.enableCopyOnReadSupport()) {
            try {
                copier = createIndexCopier(bundleContext, config);
                LOG.info("Enabling CopyOnRead support for lucene9 indexes. Index files copied under {}",
                        copier.getIndexRootDir());
            } catch (IOException e) {
                LOG.warn("Could not initialize CopyOnRead support for lucene9 indexes; " +
                        "falling back to reading directly from the remote NodeStore", e);
            }
        }
        this.indexCopier = copier;

        // Initialize tracker
        indexTracker = new LuceneNgIndexTracker(copier);

        // Register QueryIndexProvider
        LuceneNgQueryIndexProvider queryProvider = new LuceneNgQueryIndexProvider(indexTracker);
        Dictionary<String, Object> props = new Hashtable<>();
        props.put("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        regs.add(bundleContext.registerService(QueryIndexProvider.class.getName(), queryProvider, props));
        LOG.info("Registered QueryIndexProvider for type: {}", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Register IndexEditorProvider
        editorProvider = new LuceneNgIndexEditorProvider(indexTracker);
        props = new Hashtable<>();
        props.put("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        regs.add(bundleContext.registerService(IndexEditorProvider.class.getName(), editorProvider, props));
        LOG.info("Registered IndexEditorProvider for type: {}", LuceneNgIndexConstants.TYPE_LUCENE9);
    }

    private LuceneNgIndexCopier createIndexCopier(BundleContext bundleContext, Config config) throws IOException {
        String indexDirPath = config.localIndexDir();
        if (indexDirPath == null || indexDirPath.isEmpty()) {
            String repoHome = bundleContext.getProperty(REPOSITORY_HOME);
            if (repoHome == null) {
                throw new IOException("Index directory cannot be determined as neither localIndexDir " +
                        "config nor repository.home is set");
            }
            indexDirPath = Paths.get(repoHome, "index").toString();
        }
        boolean prefetchEnabled = config.prefetchIndexFiles();
        if (prefetchEnabled) {
            LOG.info("Prefetching of lucene9 index files enabled");
        }
        return new LuceneNgIndexCopier(getExecutorService(), new File(indexDirPath), prefetchEnabled);
    }

    private ExecutorService getExecutorService() {
        if (executorService == null) {
            executorService = ExecutorHelper.linkedQueueExecutor(
                    INDEX_COPIER_POOL_SIZE, "oak-lucene9-%d",
                    (t, e) -> LOG.warn("Error occurred in asynchronous lucene9 index copy processing", e));
        }
        return executorService;
    }

    @Deactivate
    private void deactivate() {
        LOG.info("Deactivating LuceneNg Index Provider");

        for (ServiceRegistration<?> reg : regs) {
            reg.unregister();
        }
        regs.clear();

        if (editorProvider != null) {
            editorProvider.close();
            editorProvider = null;
        }

        if (indexTracker != null) {
            // FulltextIndexTracker.close() is package-private to oak-search's spi.query
            // package and not reachable from here (unlike ElasticIndexTracker, this
            // tracker holds real local resources — open Lucene readers/segment files —
            // that must not be leaked on bundle deactivation). Driving update() with an
            // empty root has the same effect through the tracker's public API: every
            // currently tracked path is diffed against "removed" and, since isUpdateNeeded
            // detects the change, openIndex() is invoked (and returns null, since there is
            // no data under an empty root) so the *previous* generation's IndexNodeManager
            // is close()d (public, inherited) and releaseResources() runs.
            indexTracker.update(EmptyNodeState.EMPTY_NODE);
            indexTracker = null;
        }

        if (indexCopier != null) {
            try {
                indexCopier.close();
            } catch (IOException e) {
                LOG.warn("Error closing lucene9 IndexCopier", e);
            }
            indexCopier = null;
        }

        if (executorService != null) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executorService = null;
        }
    }
}
