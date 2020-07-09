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

import static org.apache.jackrabbit.oak.spi.toggle.Feature.newFeature;
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
        try (Feature feature = newFeature("my.toggle", whiteboard)) {
            assertFalse(feature.isEnabled());
        }
    }

    @Test
    public void register() {
        try (Feature feature = newFeature("my.toggle", whiteboard)) {
            assertFalse(feature.isEnabled());
            List<FeatureToggle> toggles = getFeatureToggles();
            assertEquals(1, toggles.size());
            FeatureToggle toggle = toggles.get(0);
            assertEquals("my.toggle", toggle.getName());

            assertFalse(toggle.setEnabled(true));
            assertTrue(toggle.isEnabled());
            assertTrue(feature.isEnabled());

            assertTrue(toggle.setEnabled(true));
            assertTrue(toggle.setEnabled(false));
            assertFalse(toggle.isEnabled());
            assertFalse(feature.isEnabled());
        }
    }

    @Test
    public void registerMultiple() {
        try (Feature f1 = newFeature("my.t1", whiteboard);
             Feature f2 = newFeature("my.t2", whiteboard)) {
            assertFalse(f1.isEnabled());
            assertFalse(f2.isEnabled());
            List<FeatureToggle> toggles = getFeatureToggles();
            assertEquals(2, toggles.size());
            List<String> toggleNames = new ArrayList<>();
            for (FeatureToggle adapter : toggles) {
                toggleNames.add(adapter.getName());
            }
            assertThat(toggleNames, hasItems("my.t1", "my.t2"));
        }
    }

    @Test
    public void unregisterOnClose() {
        List<FeatureToggle> toggles;
        try (Feature feature = newFeature("my.toggle", whiteboard)) {
            assertFalse(feature.isEnabled());
            toggles = getFeatureToggles();
            assertEquals(1, toggles.size());
        }
        toggles = getFeatureToggles();
        assertThat(toggles, is(empty()));
    }

    private List<FeatureToggle> getFeatureToggles() {
        return WhiteboardUtils.getServices(
                whiteboard, FeatureToggle.class);
    }
}
