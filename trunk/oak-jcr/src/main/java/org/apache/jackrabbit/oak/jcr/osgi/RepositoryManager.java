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
package org.apache.jackrabbit.oak.jcr.osgi;

import java.util.Map;
import java.util.Properties;

import javax.jcr.Repository;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceStrategy;
import org.apache.felix.scr.annotations.References;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.plugins.version.VersionHook;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.commit.WhiteboardEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.WhiteboardIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.query.WhiteboardIndexProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

/**
 * RepositoryManager constructs the Repository instance and registers it with OSGi Service Registry.
 * By default it would not be active and would require explicit configuration to be registered so as
 * create repository. This is done to prevent repository creation in scenarios where repository needs
 * to be configured in a custom way
 */
@Component(policy = ConfigurationPolicy.REQUIRE)
@References({
        @Reference(referenceInterface = StatisticsProvider.class,
                strategy = ReferenceStrategy.LOOKUP
        )
})
public class RepositoryManager {
    private static final int DEFAULT_OBSERVATION_QUEUE_LENGTH = BackgroundObserver.DEFAULT_QUEUE_SIZE;
    private static final boolean DEFAULT_COMMIT_RATE_LIMIT = false;
    private static final boolean DEFAULT_FAST_QUERY_RESULT_SIZE = false;

    private final WhiteboardEditorProvider editorProvider =
            new WhiteboardEditorProvider();

    private final WhiteboardIndexEditorProvider indexEditorProvider =
            new WhiteboardIndexEditorProvider();

    private final WhiteboardIndexProvider indexProvider =
            new WhiteboardIndexProvider();

    private Tracker<RepositoryInitializer> initializers;

    private Whiteboard whiteboard;

    private ServiceRegistration registration;

    private int observationQueueLength;

    private CommitRateLimiter commitRateLimiter;
    
    private boolean fastQueryResultSize;

    @Reference
    private SecurityProvider securityProvider;

    @Reference
    private NodeStore store;

    @Reference(target = "(type=property)")
    private IndexEditorProvider propertyIndex;

    @Reference(target = "(type=reference)")
    private IndexEditorProvider referenceIndex;

    @Property(
        intValue = DEFAULT_OBSERVATION_QUEUE_LENGTH,
        name = "Observation queue length",
        description = "Maximum number of pending revisions in a observation listener queue")
    private static final String OBSERVATION_QUEUE_LENGTH = "oak.observation.queue-length";

    @Property(
        boolValue = DEFAULT_COMMIT_RATE_LIMIT,
        name = "Commit rate limiter",
        description = "Limit the commit rate once the number of pending revisions in the observation " +
                "queue exceed 90% of its capacity.")
    private static final String COMMIT_RATE_LIMIT = "oak.observation.limit-commit-rate";

    @Property(
            boolValue = DEFAULT_FAST_QUERY_RESULT_SIZE,
            name = "Fast query result size",
            description = "Whether the query result size should return an estimation (or -1 if disabled) for large queries")
    private static final String FAST_QUERY_RESULT_SIZE = "oak.query.fastResultSize";

    private OsgiRepository repository;

    @Activate
    public void activate(BundleContext bundleContext, Map<String, ?> config) throws Exception {
        observationQueueLength = PropertiesUtil.toInteger(prop(
                config, bundleContext, OBSERVATION_QUEUE_LENGTH), DEFAULT_OBSERVATION_QUEUE_LENGTH);

        if(PropertiesUtil.toBoolean(prop(
                config, bundleContext, COMMIT_RATE_LIMIT), DEFAULT_COMMIT_RATE_LIMIT)) {
            commitRateLimiter = new CommitRateLimiter();
        } else {
            commitRateLimiter = null;
        }
        
        fastQueryResultSize = PropertiesUtil.toBoolean(prop(
                config, bundleContext, FAST_QUERY_RESULT_SIZE), DEFAULT_FAST_QUERY_RESULT_SIZE);
        
        whiteboard = new OsgiWhiteboard(bundleContext);
        initializers = whiteboard.track(RepositoryInitializer.class);
        editorProvider.start(whiteboard);
        indexEditorProvider.start(whiteboard);
        indexProvider.start(whiteboard);
        registration = registerRepository(bundleContext);
    }

    private static Object prop(Map<String, ?> config, BundleContext bundleContext, String name) {
        //Prefer framework property first
        Object value = bundleContext.getProperty(name);
        if (value != null) {
            return value;
        }

        //Fallback to one from config
        return config.get(name);
    }


    @Deactivate
    public void deactivate() {
        if (registration != null) {
            registration.unregister();
            registration = null;
        }

        if (repository != null) {
            repository.shutdown();
            repository = null;
        }

        initializers.stop();
        indexProvider.stop();
        indexEditorProvider.stop();
        editorProvider.stop();
    }

    private ServiceRegistration registerRepository(BundleContext bundleContext) {
        Oak oak = new Oak(store)
                .with(new InitialContent())
                .with(new VersionHook())
                .with(JcrConflictHandler.createJcrConflictHandler())
                .with(whiteboard)
                .with(securityProvider)
                .with(editorProvider)
                .with(indexEditorProvider)
                .with(indexProvider)
                .withFailOnMissingIndexProvider()
                .withAsyncIndexing();

        for(RepositoryInitializer initializer : initializers.getServices()){
            oak.with(initializer);
        }

        if (commitRateLimiter != null) {
            oak.with(commitRateLimiter);
        }

        repository = new OsgiRepository(
                oak.createContentRepository(),
                whiteboard,
                securityProvider,
                observationQueueLength,
                commitRateLimiter,
                fastQueryResultSize
        );

        return bundleContext.registerService(Repository.class.getName(), repository, new Properties());
    }
}
