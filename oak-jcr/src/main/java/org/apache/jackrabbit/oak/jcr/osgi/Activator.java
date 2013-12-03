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
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.RegistrationEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.version.VersionEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class Activator implements BundleActivator, ServiceTrackerCustomizer {

    private BundleContext context;

    private Whiteboard whiteboard;

    private SecurityProvider securityProvider = new OpenSecurityProvider(); // TODO review

    private ServiceTracker tracker;

    private final Map<ServiceReference, ServiceRegistration> services =
            new HashMap<ServiceReference, ServiceRegistration>();

    //-----------------------------------------------------< BundleActivator >--

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;
        whiteboard = new OsgiWhiteboard(context);
        tracker = new ServiceTracker(context, NodeStore.class.getName(), this);
        tracker.open();
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        tracker.close();
    }

    //--------------------------------------------< ServiceTrackerCustomizer >--

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof NodeStore) {
            // FIXME: get most of these plugins through OSGi
            ContentRepository cr = new Oak((NodeStore) service)
                .with(new InitialContent())
                .with(JcrConflictHandler.JCR_CONFLICT_HANDLER)
                .with(new EditorHook(new VersionEditorProvider()))
                .with(whiteboard)
                .with(securityProvider)
                .with(new NameValidatorProvider())
                .with(new NamespaceEditorProvider())
                .with(new TypeEditorProvider())
                .with(new RegistrationEditorProvider())
                .with(new ConflictValidatorProvider())
                .with(new ReferenceEditorProvider())
                .with(new ReferenceIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(new NodeTypeIndexProvider())
                .withAsyncIndexing()
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
