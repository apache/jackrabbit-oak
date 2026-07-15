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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.migration.report.AssertingPeriodicReporter.hasReportedNode;
import static org.apache.jackrabbit.oak.plugins.migration.report.AssertingPeriodicReporter.hasReportedNodes;
import static org.apache.jackrabbit.oak.plugins.migration.report.AssertingPeriodicReporter.hasReportedProperty;
import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class ReportingNodeStateTest {

    @Test
    public void getChildNodeReportsNode() {
        final AssertingPeriodicReporter reporter = new AssertingPeriodicReporter(10, 10);
        final NodeState nodeState = ReportingNodeState.wrap(EmptyNodeState.EMPTY_NODE, reporter);

        reporter.reset();
        for (int i = 1; i <= 20; i++) {
            nodeState.getChildNode("a" + i);
        }

        assertThat(reporter, hasReportedNode(10, "/a10"));
        assertThat(reporter, hasReportedNode(20, "/a20"));
    }

    @Test
    public void getChildNodeEntriesReportsNode() {
        final NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        for (int i = 1; i < 20; i++) {
            builder.child("a" + i);
        }

        final AssertingPeriodicReporter reporter = new AssertingPeriodicReporter(10, 10);
        final NodeState nodeState = ReportingNodeState.wrap(builder.getNodeState(), reporter);

        reporter.reset();
        int counter = 0;
        String name = "<none>";
        for (final ChildNodeEntry child : nodeState.getChildNodeEntries()) {
            if (++counter == 10) {
                name = child.getName();
                break;
            }
        }

        assertThat(reporter, hasReportedNode(10, "/" + name));
    }

    @Test
    public void getPropertyReportsProperty() {
        final NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        final String name = "meaningOfLife";
        builder.setProperty(name, "42");

        final AssertingPeriodicReporter reporter = new AssertingPeriodicReporter(1, 1);
        final NodeState nodeState = ReportingNodeState.wrap(builder.getNodeState(), reporter);

        reporter.reset();

        // 7 accesses via 7 methods
        nodeState.getProperty(name);
        nodeState.getBoolean(name);
        nodeState.getLong(name);
        nodeState.getString(name);
        nodeState.getStrings(name);
        nodeState.getName(name);
        nodeState.getNames(name);

        assertThat(reporter, not(hasReportedProperty(0, "/meaningOfLife")));
        assertThat(reporter, hasReportedProperty(1, "/meaningOfLife"));
        assertThat(reporter, hasReportedProperty(2, "/meaningOfLife"));
        assertThat(reporter, hasReportedProperty(3, "/meaningOfLife"));
        assertThat(reporter, hasReportedProperty(4, "/meaningOfLife"));
        assertThat(reporter, hasReportedProperty(5, "/meaningOfLife"));
        assertThat(reporter, hasReportedProperty(6, "/meaningOfLife"));
        assertThat(reporter, hasReportedProperty(7, "/meaningOfLife"));
        assertThat(reporter, not(hasReportedProperty(8, "/meaningOfLife")));
    }

    @Test
    public void getPropertiesReportsProperty() {
        final NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        for (int i = 0; i < 20; i++) {
            builder.setProperty("a" + i, "foo");
        }

        final AssertingPeriodicReporter reporter = new AssertingPeriodicReporter(10, 10);
        final NodeState nodeState = ReportingNodeState.wrap(builder.getNodeState(), reporter);

        reporter.reset();
        int counter = 0;
        for (final PropertyState property : nodeState.getProperties()) {
            if (++counter == 10) {
                break;
            }
        }

        assertThat(reporter, hasReportedProperty(10, any(String.class)));
    }

    @Test
    public void compareAgainstBaseState() {
        final NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();
        root.child("a").child("aa");

        final NodeState before = root.getNodeState();

        root.child("a").child("ab");
        root.child("b");

        final AssertingPeriodicReporter reporter = new AssertingPeriodicReporter(1, -1);
        final NodeState after = ReportingNodeState.wrap(root.getNodeState(), reporter);

        reporter.reset();
        NodeStateTestUtils.expectDifference()
                .childNodeAdded(
                        "/a/ab",
                        "/b"
                )
                .childNodeChanged(
                        "/a"
                )
                .strict()
                .verify(before, after);

        assertThat(reporter, hasReportedNodes("/a", "/a/ab", "/b"));
    }
}
