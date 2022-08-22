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
package org.apache.jackrabbit.oak.util;

import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;

import java.util.Map;

public class TestFeatureToggleFactory {

    static class FeatureFactoryWhiteBoard extends DefaultWhiteboard {
        final private boolean enabled;

        FeatureFactoryWhiteBoard(boolean enabled) {
            this.enabled = enabled;
        }

        @Override
        public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
            if (service instanceof FeatureToggle) {
                ((FeatureToggle) service).setEnabled(enabled);
            }
            return super.register(type, service, properties);
        }
    }

    public static Feature newFeature(String name, boolean enabled) {
        return Feature.newFeature(name, new FeatureFactoryWhiteBoard(enabled));
    }
}
