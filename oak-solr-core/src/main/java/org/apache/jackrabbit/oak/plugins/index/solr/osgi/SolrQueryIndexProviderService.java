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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Osgi Service that provides Solr based {@link org.apache.jackrabbit.oak.spi.query.QueryIndex}es
 *
 * @see org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider
 * @see QueryIndexProvider
 */
@Component(metatype = true, immediate = true, label = "Apache Jackrabbit Oak Solr Query index provider configuration")
public class SolrQueryIndexProviderService {

    private static final boolean QUERY_TIME_AGGREGATION_DEFAULT = false;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final List<ServiceRegistration> regs = Lists.newArrayList();

    @Reference
    private SolrServerProvider solrServerProvider;

    @Reference
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY,
            policyOption = ReferencePolicyOption.GREEDY,
            policy = ReferencePolicy.DYNAMIC
    )
    private volatile QueryIndex.NodeAggregator nodeAggregator;

    @Property(boolValue = QUERY_TIME_AGGREGATION_DEFAULT, label = "query time aggregation",
            description = "enable query time aggregation for Solr index")
    private static final String QUERY_TIME_AGGREGATION = "query.aggregation";

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    protected void activate(ComponentContext componentContext) {
        Object value = componentContext.getProperties().get(QUERY_TIME_AGGREGATION);
        boolean queryTimeAggregation = PropertiesUtil.toBoolean(value, QUERY_TIME_AGGREGATION_DEFAULT);
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
