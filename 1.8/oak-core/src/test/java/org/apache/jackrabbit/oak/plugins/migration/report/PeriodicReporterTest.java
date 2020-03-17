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
package org.apache.jackrabbit.oak.plugins.migration.report;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.migration.report.AssertingPeriodicReporter.hasReportedNode;
import static org.apache.jackrabbit.oak.plugins.migration.report.AssertingPeriodicReporter.hasReportedProperty;
import static org.junit.Assert.assertThat;

public class PeriodicReporterTest {

    @Test
    public void callbackEveryTenNodes() {
        final AssertingPeriodicReporter reporter = new AssertingPeriodicReporter(10, -1);
        final NodeState counter = ReportingNodeState.wrap(EmptyNodeState.EMPTY_NODE, reporter)
                .getChildNode("counter");

        reporter.reset();
        for (int i = 1; i < 40; i++) {
            counter.getChildNode(Integer.toString(i));
        }

        assertThat(reporter, hasReportedNode(10, "/counter/10"));
        assertThat(reporter, hasReportedNode(20, "/counter/20"));
        assertThat(reporter, hasReportedNode(30, "/counter/30"));
    }

    @Test
    public void callbackEveryTenProperties() {
        final NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        for (int i = 1; i < 40; i++) {
            builder.child("counter").setProperty(Integer.toString(i), i);
        }

        final AssertingPeriodicReporter reporter = new AssertingPeriodicReporter(-1, 10);
        final NodeState counter = ReportingNodeState.wrap(builder.getNodeState(), reporter)
                .getChildNode("counter");

        reporter.reset();
        for (int i = 1; i < 40; i++) {
            counter.getProperty(Integer.toString(i));
        }

        assertThat(reporter, hasReportedProperty(10, "/counter/10"));
        assertThat(reporter, hasReportedProperty(20, "/counter/20"));
        assertThat(reporter, hasReportedProperty(30, "/counter/30"));
    }

}
