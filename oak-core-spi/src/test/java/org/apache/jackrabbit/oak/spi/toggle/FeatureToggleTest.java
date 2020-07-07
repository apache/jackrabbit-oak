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
package org.apache.jackrabbit.oak.spi.toggle;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.toggle.Feature.newFeatureToggle;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FeatureToggleTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.bundleContext());

    @Test
    public void disabledByDefault() {
        try (Feature toggle = newFeatureToggle("my.toggle", whiteboard)) {
            assertFalse(toggle.isEnabled());
        }
    }

    @Test
    public void register() {
        try (Feature toggle = newFeatureToggle("my.toggle", whiteboard)) {
            assertFalse(toggle.isEnabled());
            List<FeatureToggle> adapters = getToggleAdapters();
            assertEquals(1, adapters.size());
            FeatureToggle adapter = adapters.get(0);
            assertEquals("my.toggle", adapter.getName());

            assertFalse(adapter.setEnabled(true));
            assertTrue(toggle.isEnabled());

            assertTrue(adapter.setEnabled(true));
            assertTrue(adapter.setEnabled(false));
            assertFalse(toggle.isEnabled());
        }
    }

    @Test
    public void registerMultiple() {
        try (Feature t1 = newFeatureToggle("my.t1", whiteboard);
             Feature t2 = newFeatureToggle("my.t2", whiteboard)) {
            assertFalse(t1.isEnabled());
            assertFalse(t2.isEnabled());
            List<FeatureToggle> adapters = getToggleAdapters();
            assertEquals(2, adapters.size());
            List<String> toggleNames = new ArrayList<>();
            for (FeatureToggle adapter : adapters) {
                toggleNames.add(adapter.getName());
            }
            assertThat(toggleNames, hasItems("my.t1", "my.t2"));
        }
    }

    @Test
    public void unregisterOnClose() {
        List<FeatureToggle> adapters;
        try (Feature toggle = newFeatureToggle("my.toggle", whiteboard)) {
            assertFalse(toggle.isEnabled());
            adapters = getToggleAdapters();
            assertEquals(1, adapters.size());
        }
        adapters = getToggleAdapters();
        assertThat(adapters, is(empty()));
    }

    private List<FeatureToggle> getToggleAdapters() {
        return WhiteboardUtils.getServices(
                whiteboard, FeatureToggle.class);
    }
}
