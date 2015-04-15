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
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
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
    private final List<Registration> oakRegs = Lists.newArrayList();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY,
            policy = ReferencePolicy.DYNAMIC
    )
    private NodeAggregator nodeAggregator;

    @Property(
            boolValue = false,
            label = "Enable Debug Logging",
            description = "Enables debug logging in Lucene. After enabling this actual logging can be " +
            "controlled via changing log level for category 'oak.lucene' to debug")
    private static final String PROP_DEBUG = "debug";

    @Property(
            boolValue = true,
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

    @Property(
            boolValue = true,
            label = "Open index asynchronously",
            description = "Enable opening of indexes in asynchronous mode"
    )
    private static final String PROP_ASYNC_INDEX_OPEN = "enableOpenIndexAsync";

    private Whiteboard whiteboard;

    private WhiteboardExecutor executor;

    private BackgroundObserver backgroundObserver;

    @Reference
    ScorerProviderFactory scorerFactory;

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config)
            throws NotCompliantMBeanException {
        initializeFactoryClassLoaders(getClass().getClassLoader());
        whiteboard = new OsgiWhiteboard(bundleContext);
        executor = new WhiteboardExecutor();
        executor.start(whiteboard);

        indexProvider = new LuceneIndexProvider(createTracker(bundleContext, config), scorerFactory);
        initializeLogging(config);
        initialize();

        regs.add(bundleContext.registerService(QueryIndexProvider.class.getName(), indexProvider, null));
        registerObserver(bundleContext, config);

        oakRegs.add(registerMBean(whiteboard,
                LuceneIndexMBean.class,
                new LuceneIndexMBeanImpl(indexProvider.getTracker()),
                LuceneIndexMBean.TYPE,
                "Lucene Index statistics"));
    }

    @Deactivate
    private void deactivate() {
        for (ServiceRegistration reg : regs) {
            reg.unregister();
        }

        for (Registration reg : oakRegs){
            reg.unregister();
        }

        if (backgroundObserver != null){
            backgroundObserver.close();
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
        boolean enableCopyOnRead = PropertiesUtil.toBoolean(config.get(PROP_COPY_ON_READ), true);
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
            IndexCopier copier = new IndexCopier(executor, indexDir);
            log.info("Enabling CopyOnRead support. Index files would be copied under {}", indexDir.getAbsolutePath());

            oakRegs.add(registerMBean(whiteboard,
                    CopyOnReadStatsMBean.class,
                    copier,
                    CopyOnReadStatsMBean.TYPE,
                    "CopyOnRead support statistics"));

            return new IndexTracker(copier);
        }

        return new IndexTracker();
    }

    private void registerObserver(BundleContext bundleContext, Map<String, ?> config) {
        boolean enableAsyncIndexOpen = PropertiesUtil.toBoolean(config.get(PROP_ASYNC_INDEX_OPEN), true);
        Observer observer = indexProvider;
        if (enableAsyncIndexOpen) {
            backgroundObserver = new BackgroundObserver(indexProvider, executor, 5);
            observer = backgroundObserver;
            log.info("Registering the LuceneIndexProvider as a BackgroundObserver");
        }
        regs.add(bundleContext.registerService(Observer.class.getName(), observer, null));
    }

    private void initializeFactoryClassLoaders(ClassLoader classLoader) {
        ClassLoader originalClassLoader = Thread.currentThread()
                .getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            //Access TokenizerFactory etc trigger a static initialization
            //so switch the TCCL so that static initializer picks up the right
            //classloader
            initializeFactoryClassLoaders0(classLoader);
        } catch (Throwable t) {
            log.warn("Error occurred while initializing the Lucene " +
                    "Factories", t);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    private void initializeFactoryClassLoaders0(ClassLoader classLoader) {
        //Factories use the Threads context classloader to perform SPI classes
        //lookup by default which would not work in OSGi world. So reload the
        //factories by providing the bundle classloader
        TokenizerFactory.reloadTokenizers(classLoader);
        CharFilterFactory.reloadCharFilters(classLoader);
        TokenFilterFactory.reloadTokenFilters(classLoader);
    }


    protected void bindNodeAggregator(NodeAggregator aggregator) {
        this.nodeAggregator = aggregator;
        initialize();
    }

    protected void unbindNodeAggregator(NodeAggregator aggregator) {
        this.nodeAggregator = null;
        initialize();
    }

}
