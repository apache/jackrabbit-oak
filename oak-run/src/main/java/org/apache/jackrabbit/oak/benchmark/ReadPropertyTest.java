/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.management.MBeanServer;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code ReadPropertyTest} implements a performance test, which reads
 * three properties: one with a jcr prefix, one with the empty prefix and a
 * third one, which does not exist.
 */
public class ReadPropertyTest extends AbstractTest {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Session session;

    private Node root;

    @Override
    protected void beforeSuite() throws Exception {
        session = getRepository().login(getCredentials());
        root = session.getRootNode().addNode(
                getClass().getSimpleName() + TEST_ID, "nt:unstructured");
        root.setProperty("property", "value");
        session.save();
    }

    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < 10000; i++) {
            root.getProperty("jcr:primaryType");
            root.getProperty("property");
            root.hasProperty("does-not-exist");
        }
    }

    @Override
    protected void afterSuite() throws Exception {
        root.remove();
        session.save();
        session.logout();
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture){
            return ((OakRepositoryFixture)fixture).setUpCluster(1, new JcrCreator(){
                @Override
                public Jcr customize(Oak oak) {
                    boolean enableMetrics = Boolean.getBoolean("enableMetrics");
                    if (enableMetrics) {
                        log.info("Enabling Metrics integration");
                        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                        ScheduledExecutorService executor =
                                MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));
                        MetricStatisticsProvider statsProvider = new MetricStatisticsProvider(server, executor);
                        oak.getWhiteboard().register(StatisticsProvider.class,
                                statsProvider, Collections.emptyMap());

                        final Slf4jReporter reporter = Slf4jReporter.forRegistry(statsProvider.getRegistry())
                                .outputTo(LoggerFactory.getLogger("org.apache.jackrabbit.oak.metrics"))
                                .convertRatesTo(TimeUnit.SECONDS)
                                .filter(new MetricFilter() {
                                    @Override
                                    public boolean matches(String name, Metric metric) {
                                        return name.startsWith("SESSION_READ");
                                    }
                                })
                                .convertDurationsTo(TimeUnit.MICROSECONDS)
                                .build();
                        reporter.start(30, TimeUnit.SECONDS);
                    }
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }
}
