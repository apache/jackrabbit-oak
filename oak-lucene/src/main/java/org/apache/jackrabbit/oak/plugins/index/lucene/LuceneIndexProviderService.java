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

import java.io.File;
import java.util.List;
import java.util.Map;

import javax.management.NotCompliantMBeanException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexCopier;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.InfoStream;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@SuppressWarnings("UnusedDeclaration")
@Component(metatype = true, label = "Apache Jackrabbit Oak LuceneIndexProvider")
public class LuceneIndexProviderService {
    public static final String REPOSITORY_HOME = "repository.home";

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

    @Property(
            boolValue = false,
            label = "Enable Debug Logging",
            description = "Enables debug logging in Lucene. After enabling this actual logging can be " +
            "controlled via changing log level for category 'oak.lucene' to debug")
    private static final String PROP_DEBUG = "debug";

    @Property(
            boolValue = false,
            label = "Enable CopyOnRead",
            description = "Enable copying of Lucene index to local file system to improve query performance"
    )
    private static final String PROP_COPY_ON_READ = "enableCopyOnReadSupport";

    @Property(
            label = "Local index storage path",
            description = "Local file system path where Lucene indexes would be copied when CopyOnRead is enabled. " +
                    "If not specified then indexes would be stored under 'index' dir under Repository Home"
    )
    private static final String PROP_LOCAL_INDEX_DIR = "localIndexDir";

    private Registration mbeanReg;

    private Whiteboard whiteboard;

    private WhiteboardExecutor executor;

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config)
            throws NotCompliantMBeanException {
        whiteboard = new OsgiWhiteboard(bundleContext);

        indexProvider = new LuceneIndexProvider(createTracker(bundleContext, config));
        initializeLogging(config);
        initialize();

        QueryIndexProvider aggregate = AggregateIndexProvider.wrap(indexProvider);

        regs.add(bundleContext.registerService(QueryIndexProvider.class.getName(), aggregate, null));
        regs.add(bundleContext.registerService(Observer.class.getName(), indexProvider, null));

        mbeanReg = registerMBean(whiteboard,
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

        if (executor != null){
            executor.stop();
        }

        InfoStream.setDefault(InfoStream.NO_OUTPUT);
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

    private void initializeLogging(Map<String, ?> config) {
        boolean debug = PropertiesUtil.toBoolean(config.get(PROP_DEBUG), false);
        if (debug) {
            InfoStream.setDefault(LoggingInfoStream.INSTANCE);
            log.info("Registered LoggingInfoStream with Lucene. Lucene logs can be enabled " +
                    "now via category [{}]", LoggingInfoStream.PREFIX);
        }
    }

    private IndexTracker createTracker(BundleContext bundleContext, Map<String, ?> config) {
        boolean enableCopyOnRead = PropertiesUtil.toBoolean(config.get(PROP_COPY_ON_READ), false);
        if (enableCopyOnRead){
            String indexDirPath = PropertiesUtil.toString(config.get(PROP_LOCAL_INDEX_DIR), null);
            if (Strings.isNullOrEmpty(indexDirPath)) {
                String repoHome = bundleContext.getProperty(REPOSITORY_HOME);
                if (repoHome != null){
                    indexDirPath = FilenameUtils.concat(repoHome, "index");
                }
            }

            checkNotNull(indexDirPath, "Index directory cannot be determined as neither index " +
                    "directory path [%s] nor repository home [%s] defined", PROP_LOCAL_INDEX_DIR, REPOSITORY_HOME);

            File indexDir = new File(indexDirPath);
            executor = new WhiteboardExecutor();
            executor.start(whiteboard);
            IndexCopier copier = new IndexCopier(executor, indexDir);
            log.info("Enabling CopyOnRead support. Index files would be copied under {}", indexDir.getAbsolutePath());
            return new IndexTracker(copier);
        }

        return new IndexTracker();
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
