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
package org.apache.jackrabbit.oak.run;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import org.apache.jackrabbit.oak.run.MetricsExporterFixtureProvider.ExportMetricsArgs;
import org.apache.jackrabbit.oak.run.MetricsExporterFixtureProvider.ExporterType;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.getService;

/**
 * Initializes metrics exported fixture for fullGC. For use in oak-run RevisionsCommand.
 */
class FullGCMetricsExporterFixtureProvider {

    private static final Logger log = LoggerFactory.getLogger(NodeStoreFixtureProvider.class);

    @Nullable
    static FullGCMetricsExporterFixture<PushGateway> create(RevisionsCommand.RevisionsOptions options, Whiteboard wb) {
        if (options.exportMetrics()) {
            CollectorRegistry collectorRegistry = new CollectorRegistry();
            wb.register(CollectorRegistry.class, collectorRegistry, emptyMap());

            MetricRegistry metricRegistry = getService(wb, MetricRegistry.class);

            ExportMetricsArgs metricsArgs = new ExportMetricsArgs(options.exportMetricsArgs());
            if (metricsArgs.getExporterType() == ExporterType.pushgateway) {
                PushGateway pg = new PushGateway(metricsArgs.getPushUri());
                new DropwizardExports(metricRegistry).register(collectorRegistry);

                wb.register(PushGateway.class, pg, emptyMap());
                return new FullGCMetricsExporterFixture<>() {
                    public ExporterType getExporterType() {
                        return ExporterType.pushgateway;
                    }

                    public PushGateway getMetricsExporter() {
                        return pg;
                    }

                    @Override
                    public void close() {
                        pushMetrics(collectorRegistry, pg, metricsArgs);
                    }

                    /**
                     * Push the metricsMap that is passed from VersionGarbageCollector to pushgateway.
                     */
                    @Override
                    public void onIterationComplete() {
                        pushMetrics(collectorRegistry, pg, metricsArgs);
                    }
                };
            }
        }
        return null;
    }

    private static void pushMetrics(CollectorRegistry collectorRegistry, PushGateway pg, ExportMetricsArgs metricsArgs) {
        try {
            log.info("Pushing metrics to pushgateway: ", metricsArgs.getPushMap());
            pg.pushAdd(collectorRegistry, PushGateway.class.getName(), metricsArgs.getPushMap());
        } catch (IOException e) {
            log.error("Error pushing metrics to pushgateway", e);
        }
    }
}
