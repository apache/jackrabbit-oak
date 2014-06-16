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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.lucene.analysis.Analyzer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@SuppressWarnings("UnusedDeclaration")
@Component(immediate = true)
public class LuceneIndexProviderService {

    private LuceneIndexProvider indexProvider;

    private final List<ServiceRegistration> regs = Lists.newArrayList();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Analyzer defaultAnalyzer = LuceneIndexConstants.ANALYZER;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY,
            policy = ReferencePolicy.DYNAMIC
    )
    private NodeAggregator nodeAggregator;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY,
            policy = ReferencePolicy.DYNAMIC
    )
    protected Analyzer analyzer;

    private Registration mbeanReg;

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config) {
        indexProvider = new LuceneIndexProvider();
        initialize();

        QueryIndexProvider aggregate = AggregateIndexProvider.wrap(indexProvider);

        regs.add(bundleContext.registerService(QueryIndexProvider.class.getName(), aggregate, null));
        regs.add(bundleContext.registerService(Observer.class.getName(), indexProvider, null));

        mbeanReg = registerMBean(new OsgiWhiteboard(bundleContext),
                LuceneIndexMBean.class,
                new LuceneIndexMBeanImpl(indexProvider.getTracker()),
                LuceneIndexMBean.TYPE,
                "Lucene Index statistics");
    }

    @Deactivate
    private void deactivate() {
        for (ServiceRegistration reg : regs) {
            reg.unregister();
        }

        if(mbeanReg != null){
            mbeanReg.unregister();
        }

        if (indexProvider != null) {
            indexProvider.close();
            indexProvider = null;
        }
    }

    private void initialize(){
        if(indexProvider == null){
            return;
        }

        if(nodeAggregator != null){
            log.debug("Using NodeAggregator {}", nodeAggregator.getClass());
        }

        indexProvider.setAggregator(nodeAggregator);

        Analyzer analyzer = this.analyzer != null ? this.analyzer : defaultAnalyzer;
        indexProvider.setAnalyzer(analyzer);
    }

    protected void bindNodeAggregator(NodeAggregator aggregator) {
        this.nodeAggregator = aggregator;
        initialize();
    }

    protected void unbindNodeAggregator(NodeAggregator aggregator) {
        this.nodeAggregator = null;
        initialize();
    }

    protected void bindAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        initialize();
    }

    protected void unbindAnalyzer(Analyzer analyzer) {
        this.analyzer = null;
        initialize();
    }
}
