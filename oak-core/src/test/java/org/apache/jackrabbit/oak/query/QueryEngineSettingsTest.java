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
package org.apache.jackrabbit.oak.query;

import org.apache.jackrabbit.oak.api.StrictPathRestriction;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.toggle.Feature.newFeature;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class QueryEngineSettingsTest {

    @Test
    public void strictPathRestrictionDefaultsToDisable() {
        QueryEngineSettings settings = new QueryEngineSettings();
        assertEquals(StrictPathRestriction.DISABLE.name(), settings.getStrictPathRestriction());
    }

    @Test
    public void toggleRaisesDefaultFromDisableToWarn() {
        QueryEngineSettings settings = new QueryEngineSettings();
        Whiteboard whiteboard = new DefaultWhiteboard();
        Feature feature = newFeature(QueryEngineSettings.FT_PATH_RESTRICTION_WARN_BY_DEFAULT, whiteboard);
        try {
            settings.setPathRestrictionWarnByDefaultFeature(feature);
            // registered but not yet enabled -> still the DISABLE default
            assertEquals(StrictPathRestriction.DISABLE.name(), settings.getStrictPathRestriction());

            FeatureToggle toggle = WhiteboardUtils.getService(whiteboard, FeatureToggle.class);
            assertNotNull(toggle);

            toggle.setEnabled(true);
            assertEquals(StrictPathRestriction.WARN.name(), settings.getStrictPathRestriction());

            toggle.setEnabled(false);
            assertEquals(StrictPathRestriction.DISABLE.name(), settings.getStrictPathRestriction());
        } finally {
            feature.close();
        }
    }

    @Test
    public void toggleDoesNotOverrideExplicitSetting() {
        QueryEngineSettings settings = new QueryEngineSettings();
        Whiteboard whiteboard = new DefaultWhiteboard();
        Feature feature = newFeature(QueryEngineSettings.FT_PATH_RESTRICTION_WARN_BY_DEFAULT, whiteboard);
        try {
            settings.setPathRestrictionWarnByDefaultFeature(feature);
            settings.setStrictPathRestriction(StrictPathRestriction.ENABLE.name());

            FeatureToggle toggle = WhiteboardUtils.getService(whiteboard, FeatureToggle.class);
            assertNotNull(toggle);
            toggle.setEnabled(true);

            // an explicitly configured value is not raised to WARN by the toggle
            assertEquals(StrictPathRestriction.ENABLE.name(), settings.getStrictPathRestriction());
        } finally {
            feature.close();
        }
    }
}
