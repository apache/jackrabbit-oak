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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Splitter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.getService;

/**
 * Initialize different metrics exporter fixture based on parameters used.
 */
public class MetricsExporterFixtureProvider {
    private static final Logger log = LoggerFactory.getLogger(NodeStoreFixtureProvider.class);

    @Nullable
    public static MetricsExporterFixture create(DataStoreOptions options, Whiteboard wb) throws Exception {
        if (options.exportMetrics()) {
            CollectorRegistry collectorRegistry = new CollectorRegistry();
            wb.register(CollectorRegistry.class, collectorRegistry, emptyMap());

            MetricRegistry metricRegistry = getService(wb, MetricRegistry.class);

            ExportMetricsArgs
                metricsArgs = new ExportMetricsArgs(options.exportMetricsArgs());
            if (metricsArgs.getExporterType() == ExporterType.pushgateway) {
                PushGateway pg = new PushGateway(metricsArgs.getPushUri());
                new DropwizardExports(metricRegistry).register(collectorRegistry);

                wb.register(PushGateway.class, pg, emptyMap());
                return new MetricsExporterFixture<PushGateway>() {
                    @Override public ExporterType getExporterType() {
                        return ExporterType.pushgateway;
                    }

                    @Override public PushGateway getMetricsExporter() {
                        return pg;
                    }

                    @Override public void close() throws IOException {
                        pg.pushAdd(collectorRegistry, PushGateway.class.getName(), metricsArgs.getPushMap());
                    }
                };
            }
        }
        return null;
    }

    /**
     * Metrics exporter arguments
     *  1. Exporter Type
     *  2. Exporter URI
     *  3. Metadata map
     *  e.g. pushgateway;uri;key1=value1,key2=value2
     */
    static class ExportMetricsArgs {
        private final ExporterType exporterType;
        private final String pushUri;
        private final Map<String, String> pushMap;

        ExportMetricsArgs(String args) {
            List<String> split = Splitter.on(";").limit(3).omitEmptyStrings().trimResults().splitToList(args);
            this.exporterType = ExporterType.valueOf(split.get(0));

            if (split.size() < 2) {
                throw new IllegalArgumentException("No URL defined");
            }

            this.pushUri = split.get(1);

            if (split.size() > 2) {
                this.pushMap = Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(split.get(2));
            } else {
                this.pushMap = emptyMap();
            }
            log.info("Map of properties pushed [{}]", pushMap);
        }

        public String getPushUri() {
            return pushUri;
        }

        public Map<String, String> getPushMap() {
            return pushMap;
        }

        public ExporterType getExporterType() {
            return exporterType;
        }
    }


    /**
     * Exporter Type supported
     */
    public enum ExporterType {
        pushgateway("Prometheus Push Gateway");

        private String type;

        ExporterType(String type) {
            this.type = type;
        }
    }
}
