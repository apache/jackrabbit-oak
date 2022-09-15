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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ProtectionConfig;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.osgi.util.tracker.ServiceTracker;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

class ProtectionConfigTracker extends ServiceTracker implements ProtectionConfig {
    
    public ProtectionConfigTracker(@NotNull BundleContext context) {
        super(context, ProtectionConfig.class.getName(), null);
    }

    //-------------------------------------------------------------------------------------------< ProtectionConfig >---

    @Override
    public boolean isProtectedProperty(@NotNull Tree parent, @NotNull PropertyState property) {
        for (ProtectionConfig pe : getProtectionConfigs()) {
            if (pe.isProtectedProperty(parent, property)) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public boolean isProtectedTree(@NotNull Tree tree) {
        for (ProtectionConfig pe : getProtectionConfigs()) {
            if (pe.isProtectedTree(tree)) {
                return true;
            }
        }
        return false;
    }

    //------------------------------------------------------------------------------------------------------------------

    private @NotNull Iterable<ProtectionConfig> getProtectionConfigs() {
        Object[] services = getServices();
        if (services == null) {
            return Collections.singletonList(ProtectionConfig.DEFAULT);
        } else {
            return Arrays.stream(services).map(ProtectionConfig.class::cast).collect(Collectors.toList());
        }
    }
}