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

import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * OSGi service that activates the Lucene 9 index provider stack.
 *
 * <p>On activation this registers:</p>
 * <ul>
 *   <li>{@link QueryIndexProvider} — serves lucene9 queries</li>
 *   <li>{@link Observer} (wrapped in {@link BackgroundObserver}) — refreshes the
 *       tracker on every commit so that queries always see up-to-date index data</li>
 *   <li>{@link IndexEditorProvider} — handles writes for {@code type=lucene9} index
 *       definitions</li>
 * </ul>
 */
@Component
@Designate(ocd = LuceneNgIndexProviderService.Config.class)
public class LuceneNgIndexProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexProviderService.class);

    /** Queue depth for the background observer (same default as oak-lucene). */
    private static final int OBSERVER_QUEUE_SIZE = 1000;

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak LuceneNg Index Provider",
            description = "Lucene 9 index provider for Oak"
    )
    public @interface Config {
        @AttributeDefinition(
                name = "Disable this component",
                description = "If true, this component is disabled."
        )
        boolean disabled() default false;
    }

    private final List<ServiceRegistration<?>> regs = new ArrayList<>();
    private LuceneNgIndexTracker indexTracker;
    private LuceneNgIndexEditorProvider editorProvider;
    private BackgroundObserver backgroundObserver;
    private ExecutorService executor;

    @Activate
    private void activate(BundleContext bundleContext, Config config) {
        if (config.disabled()) {
            LOG.info("LuceneNg component disabled by configuration");
            return;
        }

        LOG.info("Activating LuceneNg Index Provider");

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oak-lucene9-observer");
            t.setDaemon(true);
            return t;
        });

        indexTracker = new LuceneNgIndexTracker();

        // QueryIndexProvider + Observer in one object
        LuceneNgQueryIndexProvider queryProvider = new LuceneNgQueryIndexProvider(indexTracker);

        regs.add(bundleContext.registerService(
                QueryIndexProvider.class.getName(), queryProvider, null));
        LOG.debug("Registered QueryIndexProvider for type: {}", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Wrap in BackgroundObserver so commits are not blocked by tracker refresh
        backgroundObserver = new BackgroundObserver(queryProvider, executor, OBSERVER_QUEUE_SIZE);
        regs.add(bundleContext.registerService(
                Observer.class.getName(), backgroundObserver, null));
        LOG.debug("Registered BackgroundObserver for tracker refresh");

        // IndexEditorProvider
        editorProvider = new LuceneNgIndexEditorProvider(indexTracker);
        Dictionary<String, Object> editorProps = new Hashtable<>();
        editorProps.put("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        editorProps.put("leaf", Boolean.TRUE);
        regs.add(bundleContext.registerService(
                IndexEditorProvider.class.getName(), editorProvider, editorProps));
        LOG.debug("Registered IndexEditorProvider (leaf) for type: {}", LuceneNgIndexConstants.TYPE_LUCENE9);

        LOG.info("LuceneNg Index Provider activated");
    }

    @Deactivate
    private void deactivate() {
        LOG.info("Deactivating LuceneNg Index Provider");

        for (ServiceRegistration<?> reg : regs) {
            reg.unregister();
        }
        regs.clear();

        if (backgroundObserver != null) {
            backgroundObserver.close();
            backgroundObserver = null;
        }

        if (editorProvider != null) {
            editorProvider.close();
            editorProvider = null;
        }

        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }

        if (indexTracker != null) {
            indexTracker.close();
            indexTracker = null;
        }
        LOG.info("LuceneNg Index Provider deactivated");
    }
}
