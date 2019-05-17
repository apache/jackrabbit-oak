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
package org.apache.jackrabbit.oak.spi.query;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.RuntimeNodeTraversalException;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.query.Query;
import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@code SlowQueryMetricTest} contains slowQuery metrics related tests.
 */
public class SlowQueryMetricTest {

    private ContentRepository repository;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private MetricStatisticsProvider statsProvider =
            new MetricStatisticsProvider(ManagementFactory.getPlatformMBeanServer(), executor);
    private QueryEngineSettings queryEngineSettings = new QueryEngineSettings(statsProvider);


    Oak oak = null;

    @Before
    public void setUp() {
        queryEngineSettings.setLimitReads(11);
        Whiteboard whiteboard = new DefaultWhiteboard();
        whiteboard.register(StatisticsProvider.class, statsProvider, Collections.emptyMap());
        oak = new Oak().with(new OpenSecurityProvider()).with(new InitialContent()).with(queryEngineSettings).with(whiteboard);
        repository = oak.createContentRepository();
    }

    @After
    public void tearDown() {
        repository = null;
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    private String SLOW_QUERY_COUNT_NAME = "SLOW_QUERY_COUNT";
    private String SLOW_QUERY_PERCENTILE_METRICS_NAME = "SLOW_QUERY_PERCENTILE_METRICS";

    @Test
    public void queryOnStableRevision() throws Exception {
        long maxReadEntries = 1000;//we check for max traversals for each 1000 node reads, see Cursors.java -> fetchNext()
        ContentSession s = repository.login(null, null);
        Root r = s.getLatestRoot();
        Tree t = r.getTree("/").addChild("test");
        for (int i = 0; i < maxReadEntries + 1; i++) {
            t.addChild("node" + i).setProperty("jcr:primaryType", "nt:base");
        }
        r.commit();
        ContentSession s2 = repository.login(null, null);
        Root r2 = s2.getLatestRoot();
        CounterStats slowQueryCounter = queryEngineSettings.getStatisticsProvider().getCounterStats(SLOW_QUERY_COUNT_NAME, StatsOptions.METRICS_ONLY);
        HistogramStats histogramStats = queryEngineSettings.getStatisticsProvider().getHistogram(SLOW_QUERY_PERCENTILE_METRICS_NAME, StatsOptions.METRICS_ONLY);
        long totalQueryCount = histogramStats.getCount();
        long slowQueryCount = slowQueryCounter.getCount();
        Assert.assertEquals(totalQueryCount, 0);
        Assert.assertEquals(slowQueryCount, 0);

        Result result = executeQuery(r2, "test/node1//element(*, nt:base)");
        for (ResultRow rr : result.getRows()) {
        }
        totalQueryCount = histogramStats.getCount();
        slowQueryCount = slowQueryCounter.getCount();
        Assert.assertEquals(totalQueryCount, 1);
        Assert.assertEquals(slowQueryCount, 0);

        executeAndAssertSlowQuery(r2, queryEngineSettings);
    }

    private void executeAndAssertSlowQuery(Root r2, QueryEngineSettings queryEngineSettings) throws ParseException {
        Result result = executeQuery(r2, "test//element(*, nt:base)");
        CounterStats slowQueryCounter = queryEngineSettings.getStatisticsProvider().getCounterStats(SLOW_QUERY_COUNT_NAME, StatsOptions.METRICS_ONLY);
        HistogramStats histogramStats = queryEngineSettings.getStatisticsProvider().getHistogram(SLOW_QUERY_PERCENTILE_METRICS_NAME, StatsOptions.METRICS_ONLY);
        long initialSlowQueryCounter = slowQueryCounter.getCount();
        long initialHistogramCounter = histogramStats.getCount();
        try {
            for (ResultRow rr : result.getRows()) {
            }
        } catch (RuntimeNodeTraversalException e) {

            /*
             count increased by 2. one for being a query and one for being slow query. Added twice to get histogram percentile info
             */
            Assert.assertEquals(histogramStats.getCount(), initialHistogramCounter + 2);
            Assert.assertEquals(slowQueryCounter.getCount(), initialSlowQueryCounter + 1);
            return;
        }
        Assert.fail("Unable to catch max Node Traversal limit breach");
    }

    private Result executeQuery(Root r2, String queryString) throws ParseException {
        Result result = r2.getQueryEngine().executeQuery(queryString, Query.XPATH,
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        return result;
    }
}
