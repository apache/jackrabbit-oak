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

import java.util.Properties;

import javax.jcr.Repository;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
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

    private final WhiteboardEditorProvider editorProvider =
            new WhiteboardEditorProvider();

    private final WhiteboardIndexEditorProvider indexEditorProvider =
            new WhiteboardIndexEditorProvider();

    private final WhiteboardIndexProvider indexProvider =
            new WhiteboardIndexProvider();

    private final WhiteboardExecutor executor = new WhiteboardExecutor();

    private Whiteboard whiteboard;

    private ServiceRegistration registration;

    @Reference
    private SecurityProvider securityProvider;

    @Reference
    private NodeStore store;

    @Activate
    public void activate(BundleContext bundleContext) {
        whiteboard = new OsgiWhiteboard(bundleContext);
        editorProvider.start(whiteboard);
        indexEditorProvider.start(whiteboard);
        indexProvider.start(whiteboard);
        executor.start(whiteboard);
        registration = registerRepository(bundleContext);
    }

    @Deactivate
    public void deactivate() {
        if (registration != null) {
            registration.unregister();
        }

        executor.stop();
        indexProvider.stop();
        indexEditorProvider.stop();
        editorProvider.stop();
    }

    private ServiceRegistration registerRepository(BundleContext bundleContext) {
        ContentRepository cr = new Oak(store)
                .with(new InitialContent())
                .with(JcrConflictHandler.JCR_CONFLICT_HANDLER)
                .with(whiteboard)
                .with(securityProvider)
                .with(editorProvider)
                .with(indexEditorProvider)
                .with(indexProvider)
                .withAsyncIndexing()
                .with(executor)
                .createContentRepository();

        return bundleContext.registerService(
                Repository.class.getName(),
                new OsgiRepository(cr, whiteboard, securityProvider),
                new Properties());
    }
}
