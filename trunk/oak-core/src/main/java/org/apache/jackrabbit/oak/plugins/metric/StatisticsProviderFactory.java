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

package org.apache.jackrabbit.oak.plugins.metric;

import java.io.Closeable;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.MBeanServer;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create StatisticsProvider depending on setup. It detects if the
 * <a href="http://metrics.dropwizard.io">Metrics</a> library is present or not. If present
 * then it configures a MetricsStatisticsProvider otherwise fallbacks to DefaultStatisticsProvider
 */
@Component
@Designate(ocd = StatisticsProviderFactory.Configuration.class)
public class StatisticsProviderFactory {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak StatisticsProviderFactory",
            description = "Creates a statistics providers used by Oak. By default if checks if Metrics (" +
                    "See http://metrics.dropwizard.io) library is present then that is used. Otherwise it fallbacks " +
                    "to default"
    )
    @interface Configuration {

        @AttributeDefinition(options = {
                    @Option(label = TYPE_DEFAULT, value = TYPE_DEFAULT),
                    @Option(label = TYPE_METRIC, value = TYPE_METRIC),
                    @Option(label = TYPE_NONE, value = TYPE_NONE)
                })
        String providerType() default TYPE_AUTO;

    }

    private static final String TYPE_DEFAULT = "DEFAULT";
    private static final String TYPE_METRIC = "METRIC";
    private static final String TYPE_NONE = "NONE";
    private static final String TYPE_AUTO = "AUTO";
    private static final String METRIC_PROVIDER_CLASS =
            "com.codahale.metrics.MetricRegistry";


    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Keeping this as optional as for default case MBeanServer is not required
     * Further Metrics would bound to default platform MBeanServer is no explicit
     * server is provided.
     */
    @Reference(cardinality = ReferenceCardinality.OPTIONAL)
    private MBeanServer server;
    
    private StatisticsProvider statisticsProvider;
    private List<ServiceRegistration> regs = Lists.newArrayList();
    private ScheduledExecutorService executor;
    private BundleContext bundleContext;

    @Activate
    private void activate(BundleContext context, Configuration config) {
        this.bundleContext = context;
        statisticsProvider = createProvider(config.providerType());

        if (statisticsProvider != null) {
            regs.add(context.registerService(StatisticsProvider.class.getName(),
                    statisticsProvider, null));
        }
    }

    @Deactivate
    private void deactivate() throws IOException {
        for (ServiceRegistration reg : regs){
            reg.unregister();
        }
        regs.clear();

        if (statisticsProvider instanceof Closeable) {
            ((Closeable) statisticsProvider).close();
        }

        new ExecutorCloser(executor).close();
    }

    private StatisticsProvider createProvider(String providerType) {
        if (TYPE_NONE.equals(providerType)) {
            log.info("No statistics provider created as {} option is selected", TYPE_NONE);
            return null;
        }

        executor = Executors.newSingleThreadScheduledExecutor();

        String effectiveProviderType = providerType;
        if (TYPE_AUTO.equals(providerType) && isMetricSupportPresent()) {
            effectiveProviderType = TYPE_METRIC;
        }

        if (TYPE_METRIC.equals(effectiveProviderType)) {
            log.info("Using MetricsStatisticsProvider");
            return createMetricsProvider(executor);
        }

        log.info("Using DefaultStatisticsProvider");
        return new DefaultStatisticsProvider(executor);
    }

    private StatisticsProvider createMetricsProvider(ScheduledExecutorService executor) {
        org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider metricProvider =
         new org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider(server, executor);
        Dictionary<Object, Object> dictionary = new Hashtable<Object, Object>();
        dictionary.put("name", "oak");
        regs.add(bundleContext.registerService("com.codahale.metrics.MetricRegistry",
                metricProvider.getRegistry(),  dictionary));
        return metricProvider;
    }

    private boolean isMetricSupportPresent() {
        try {
            StatisticsProviderFactory.class.getClassLoader().loadClass(METRIC_PROVIDER_CLASS);
        } catch (Throwable e) {
            log.debug("Cannot load optional Metrics library support", e);
            return false;
        }
        return true;
    }
}
