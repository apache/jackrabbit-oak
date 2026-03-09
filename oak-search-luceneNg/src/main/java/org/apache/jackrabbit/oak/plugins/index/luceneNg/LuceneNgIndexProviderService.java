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
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
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

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;

/**
 * OSGi service that provides Lucene 9 index providers.
 * This service registers both the QueryIndexProvider and IndexEditorProvider
 * for handling indexes with type "lucene9".
 */
@Component
@Designate(ocd = LuceneNgIndexProviderService.Config.class)
public class LuceneNgIndexProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgIndexProviderService.class);

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
    }

    @Reference
    private NodeStore nodeStore;

    private final List<ServiceRegistration<?>> regs = new ArrayList<>();
    private LuceneNgIndexTracker indexTracker;
    private LuceneNgIndexEditorProvider editorProvider;

    @Activate
    private void activate(BundleContext bundleContext, Config config) {
        if (config.disabled()) {
            LOG.info("LuceneNg component disabled by configuration");
            return;
        }

        LOG.info("Activating LuceneNg Index Provider");

        // Initialize tracker
        indexTracker = new LuceneNgIndexTracker();

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

        indexTracker = null;
    }
}
