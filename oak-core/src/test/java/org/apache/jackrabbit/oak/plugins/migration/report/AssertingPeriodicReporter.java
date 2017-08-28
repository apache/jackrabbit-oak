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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

class AssertingPeriodicReporter extends PeriodicReporter {

    private final Map<Long, String> reportedNodes = new HashMap<Long, String>();

    private final Map<Long, String> reportedProperties = new HashMap<Long, String>();

    public AssertingPeriodicReporter(final int nodeLogInterval, final int propertyLogInterval) {
        super(nodeLogInterval, propertyLogInterval);
    }

    @Override
    public void reset() {
        super.reset();
        reportedNodes.clear();
        reportedProperties.clear();
    }

    @Override
    protected void reportPeriodicNode(final long count, @Nonnull final ReportingNodeState nodeState) {
        reportedNodes.put(count, nodeState.getPath());
    }

    @Override
    protected void reportPeriodicProperty(final long count, @Nonnull final ReportingNodeState parent, @Nonnull final String propertyName) {
        reportedProperties.put(count, PathUtils.concat(parent.getPath(), propertyName));
    }

    @Override
    public String toString() {
        return "Reported{ nodes: " + reportedNodes + " properties: " + reportedProperties + "}";
    }

    public static Matcher<AssertingPeriodicReporter> hasReportedNode(final int count, final String path) {
        return hasReports(hasEntry((long) count, path), whatever());
    }

    public static Matcher<AssertingPeriodicReporter> hasReportedNode(final int count, final Matcher<String> pathMatcher) {
        return hasReports(typesafeHasEntry(equalTo((long) count), pathMatcher), whatever());
    }

    public static Matcher<AssertingPeriodicReporter> hasReportedNodes(final String... paths) {
        final List<Matcher<? super AssertingPeriodicReporter>> matchers =
                new ArrayList<Matcher<? super AssertingPeriodicReporter>>();
        for (final String path : paths) {
            matchers.add(hasReports(typesafeHasEntry(any(Long.class), equalTo(path)), whatever()));
        }
        return allOf(matchers);
    }

    public static Matcher<AssertingPeriodicReporter> hasReportedProperty(final int count, final String path) {
        return hasReports(whatever(), hasEntry((long) count, path));
    }

    public static Matcher<AssertingPeriodicReporter> hasReportedProperty(final int count, final Matcher<String> pathMatcher) {
        return hasReports(whatever(), typesafeHasEntry(equalTo((long) count), pathMatcher));
    }

    public static Matcher<AssertingPeriodicReporter> hasReportedProperty(final String... paths) {
        final List<Matcher<? super String>> pathMatchers = new ArrayList<Matcher<? super String>>();
        for (final String path : paths) {
            pathMatchers.add(equalTo(path));
        }
        return hasReports(whatever(), typesafeHasEntry(any(Long.class), allOf(pathMatchers)));
    }

    private static Matcher<Map<? extends Long, ? extends String>> whatever() {
        return anyOf(typesafeHasEntry(any(Long.class), any(String.class)), anything());
    }

    private static Matcher<AssertingPeriodicReporter> hasReports(
            final Matcher<Map<? extends Long, ? extends String>> nodeMapMatcher,
            final Matcher<Map<? extends Long, ? extends String>> propertyMapMatcher) {

        return new org.hamcrest.TypeSafeMatcher<AssertingPeriodicReporter>() {
            @Override
            protected boolean matchesSafely(final AssertingPeriodicReporter reporter) {
                final boolean nodesMatch = nodeMapMatcher.matches(reporter.reportedNodes);
                final boolean propertiesMatch = propertyMapMatcher.matches(reporter.reportedProperties);
                return nodesMatch && propertiesMatch;
            }

            @Override
            public void describeTo(final Description description) {
                description
                        .appendText("Reported{ nodes: ")
                        .appendDescriptionOf(nodeMapMatcher)
                        .appendText(", properties: ")
                        .appendDescriptionOf(propertyMapMatcher);

            }
        };
    }

    // Java 6 fails to infer generics correctly if hasEntry is not wrapped in this
    // method.
    private static Matcher<Map<? extends Long, ? extends String>> typesafeHasEntry(
            final Matcher<Long> countMatcher, final Matcher<String> pathMatcher) {
        return hasEntry(countMatcher, pathMatcher);
    }
}
