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
package org.apache.jackrabbit.oak.jcr.osgi;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jcr.Repository;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardEditorProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexProvider;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class Activator implements BundleActivator, ServiceTrackerCustomizer {

    private BundleContext context;

    private ServiceTracker tracker;

    private final Map<ServiceReference, ServiceRegistration> services =
            new HashMap<ServiceReference, ServiceRegistration>();

    private final WhiteboardEditorProvider editorProvider =
            new WhiteboardEditorProvider();

    private final WhiteboardIndexEditorProvider indexEditorProvider =
            new WhiteboardIndexEditorProvider();

    private final WhiteboardIndexProvider indexProvider =
            new WhiteboardIndexProvider();

    private final WhiteboardExecutor executor = new WhiteboardExecutor();

    // FIXME OAK-1476 : SecurityProvider implementation should not be hardcoded
    private final SecurityProvider securityProvider =
            new SecurityProviderImpl(buildSecurityConfig());

    private static ConfigurationParameters buildSecurityConfig() {
        // FIXME OAK-1476: review configuration default values which are Adobe specific
        Map<String, Object> userConfig = new HashMap<String, Object>();
        userConfig.put(UserConstants.PARAM_GROUP_PATH, "/home/groups");
        userConfig.put(UserConstants.PARAM_USER_PATH, "/home/users");
        userConfig.put(UserConstants.PARAM_DEFAULT_DEPTH, 1);
        userConfig.put(AccessControlAction.USER_PRIVILEGE_NAMES, new String[] {PrivilegeConstants.JCR_ALL});
        userConfig.put(AccessControlAction.GROUP_PRIVILEGE_NAMES, new String[] {PrivilegeConstants.JCR_READ});
        userConfig.put(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT);

        Map<String, Object> config = new HashMap<String, Object>();
        config.put(
                UserConfiguration.NAME,
                ConfigurationParameters.of(userConfig));
        return ConfigurationParameters.of(config);
    }

    //-----------------------------------------------------< BundleActivator >--

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;
        tracker = new ServiceTracker(context, NodeStore.class.getName(), this);
        tracker.open();

        Whiteboard whiteboard = new OsgiWhiteboard(bundleContext);
        editorProvider.start(whiteboard);
        indexEditorProvider.start(whiteboard);
        indexProvider.start(whiteboard);
        executor.start(whiteboard);
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        executor.stop();
        indexProvider.stop();
        indexEditorProvider.stop();
        editorProvider.stop();

        tracker.close();
    }

    //--------------------------------------------< ServiceTrackerCustomizer >--

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof NodeStore) {
            Whiteboard whiteboard = new OsgiWhiteboard(context);

            ContentRepository cr = new Oak((NodeStore) service)
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

            services.put(reference, context.registerService(
                    Repository.class.getName(),
                    new OsgiRepository(cr, whiteboard, securityProvider),
                    new Properties()));
            return service;
        } else {
            context.ungetService(reference);
            return null;
        }
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        services.get(reference).unregister();
        context.ungetService(reference);
    }

}
