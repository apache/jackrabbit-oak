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
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardEditorProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

/**
 * RepositoryManager constructs the Repository instance and registers it with OSGi Service Registry.
 * By default it would not be active and would require explicit configuration to be registered so as
 * create repository. This is done to prevent repository creation in scenarios where repository needs
 * to be configured in a custom way
 */
@Component(policy = ConfigurationPolicy.REQUIRE)
public class RepositoryManager {
    private static final int DEFAULT_OBSERVATION_QUEUE_LENGTH = 1000;
    private static final boolean DEFAULT_COMMIT_RATE_LIMIT = false;

    //TODO Exposed for testing purpose due to SLING-4472
    static boolean ignoreFrameworkProperties = false;

    private final WhiteboardEditorProvider editorProvider =
            new WhiteboardEditorProvider();

    private final WhiteboardIndexEditorProvider indexEditorProvider =
            new WhiteboardIndexEditorProvider();

    private final WhiteboardIndexProvider indexProvider =
            new WhiteboardIndexProvider();

    private Whiteboard whiteboard;

    private ServiceRegistration registration;

    private int observationQueueLength;

    private CommitRateLimiter commitRateLimiter;

    @Reference
    private SecurityProvider securityProvider;

    @Reference
    private NodeStore store;

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

        whiteboard = new OsgiWhiteboard(bundleContext);
        editorProvider.start(whiteboard);
        indexEditorProvider.start(whiteboard);
        indexProvider.start(whiteboard);
        registration = registerRepository(bundleContext);
    }

    private static Object prop(Map<String, ?> config, BundleContext bundleContext, String name) {
        if (!ignoreFrameworkProperties) {
            //Prefer framework property first
            Object value = bundleContext.getProperty(name);
            if (value != null) {
                return value;
            }
        }

        //Fallback to one from config
        return config.get(name);
    }


    @Deactivate
    public void deactivate() {
        if (registration != null) {
            registration.unregister();
        }

        indexProvider.stop();
        indexEditorProvider.stop();
        editorProvider.stop();
    }

    private ServiceRegistration registerRepository(BundleContext bundleContext) {
        Oak oak = new Oak(store)
                .with(new InitialContent())
                .with(JcrConflictHandler.JCR_CONFLICT_HANDLER)
                .with(whiteboard)
                .with(securityProvider)
                .with(editorProvider)
                .with(indexEditorProvider)
                .with(indexProvider)
                .withAsyncIndexing();

        if (commitRateLimiter != null) {
            oak.with(commitRateLimiter);
        }

        return bundleContext.registerService(
                Repository.class.getName(),
                new OsgiRepository(oak.createContentRepository(), whiteboard, securityProvider,
                        observationQueueLength, commitRateLimiter),
                new Properties());
    }
}
