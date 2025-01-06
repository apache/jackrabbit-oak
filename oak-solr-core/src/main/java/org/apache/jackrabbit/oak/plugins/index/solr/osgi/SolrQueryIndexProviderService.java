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
package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import java.util.ArrayList;
import java.util.List;

import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Osgi Service that provides Solr based {@link org.apache.jackrabbit.oak.spi.query.QueryIndex}es
 *
 * @see org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider
 * @see QueryIndexProvider
 */
@Component(
        immediate = true
)
@Designate(
        ocd = SolrQueryIndexProviderService.Configuration.class
)
public class SolrQueryIndexProviderService {

    @ObjectClassDefinition(
            id = "org.apache.jackrabbit.oak.plugins.index.solr.osgi.SolrQueryIndexProviderService",
            name = "Apache Jackrabbit Oak Solr Query index provider configuration"
    )
    @interface Configuration {
        @AttributeDefinition(
                name ="query time aggregation",
                description = "enable query time aggregation for Solr index"
        )
        boolean query_aggregation() default QUERY_TIME_AGGREGATION_DEFAULT;
    }

    private static final boolean QUERY_TIME_AGGREGATION_DEFAULT = true;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final List<ServiceRegistration> regs = new ArrayList<>();

    @Reference
    private SolrServerProvider solrServerProvider;

    @Reference
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    @Reference(cardinality = ReferenceCardinality.MANDATORY,
            policyOption = ReferencePolicyOption.GREEDY,
            policy = ReferencePolicy.DYNAMIC
    )
    private volatile QueryIndex.NodeAggregator nodeAggregator;

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    protected void activate(ComponentContext componentContext, Configuration configuration) {
        boolean queryTimeAggregation = configuration.query_aggregation();
        if (solrServerProvider != null && oakSolrConfigurationProvider != null) {
            QueryIndexProvider solrQueryIndexProvider = new SolrQueryIndexProvider(solrServerProvider,
                    oakSolrConfigurationProvider, nodeAggregator);
            log.debug("creating Solr query index provider {} query time aggregation", queryTimeAggregation ? "with" : "without");
            if (queryTimeAggregation) {
                solrQueryIndexProvider = AggregateIndexProvider.wrap(solrQueryIndexProvider);
            }

            regs.add(componentContext.getBundleContext().registerService(QueryIndexProvider.class.getName(), solrQueryIndexProvider, null));
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    protected void deactivate() {

        for (ServiceRegistration registration : regs) {
            registration.unregister();
        }
    }

}
