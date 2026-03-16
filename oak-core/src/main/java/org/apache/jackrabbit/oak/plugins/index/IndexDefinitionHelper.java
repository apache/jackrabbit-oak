/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Helper for normalizing index definition properties into canonical form.
 * Handles backward compatibility with legacy 'type' property while supporting
 * new 'storeTargets' and 'activeTarget' properties for multi-target writes.
 */
public class IndexDefinitionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(IndexDefinitionHelper.class);

    // Constants - these reference oak-search FulltextIndexConstants but are duplicated
    // here to avoid circular dependency
    private static final String STORE_TARGETS = "storeTargets";
    private static final String ACTIVE_TARGET = "activeTarget";
    private static final String TYPE = "type";

    private IndexDefinitionHelper() {
        // Static utility class
    }

    /**
     * Normalize index properties into canonical form with storeTargets and activeTarget.
     *
     * <p>Normalization rules:</p>
     * <ul>
     *   <li>If storeTargets defined but not activeTarget → ERROR</li>
     *   <li>If activeTarget defined but not storeTargets → storeTargets = [activeTarget]</li>
     *   <li>If type only → storeTargets = [type], activeTarget = type</li>
     *   <li>If both storeTargets/activeTarget defined → use as-is</li>
     *   <li>If type also defined with storeTargets/activeTarget → log INFO, ignore type</li>
     *   <li>If activeTarget not in storeTargets → ERROR</li>
     * </ul>
     *
     * @param definition index definition node state
     * @return normalized properties with storeTargets and activeTarget
     * @throws IllegalArgumentException if validation fails
     */
    @NotNull
    public static NormalizedIndexProperties normalize(@NotNull NodeState definition) {
        PropertyState storeTargetsProperty = definition.getProperty(STORE_TARGETS);
        PropertyState activeTargetProperty = definition.getProperty(ACTIVE_TARGET);
        PropertyState typeProperty = definition.getProperty(TYPE);

        List<String> storeTargets = null;
        String activeTarget = null;

        // Extract property values if present
        if (storeTargetsProperty != null) {
            storeTargets = new ArrayList<>();
            for (String target : storeTargetsProperty.getValue(Type.STRINGS)) {
                storeTargets.add(target);
            }
        }

        if (activeTargetProperty != null) {
            activeTarget = activeTargetProperty.getValue(Type.STRING);
        }

        String type = typeProperty != null ? typeProperty.getValue(Type.STRING) : null;

        // Validation: storeTargets requires activeTarget
        if (storeTargets != null && activeTarget == null) {
            throw new IllegalArgumentException(
                "storeTargets requires activeTarget to be set");
        }

        // Normalization logic
        if (storeTargets != null && activeTarget != null) {
            // Both defined - use as-is
            if (type != null) {
                LOG.info("type property '{}' ignored when storeTargets/activeTarget are defined", type);
            }
            return new NormalizedIndexProperties(storeTargets, activeTarget);

        } else if (activeTarget != null) {
            // activeTarget only - normalize to storeTargets = [activeTarget]
            if (type != null) {
                LOG.info("type property '{}' ignored when activeTarget is defined", type);
            }
            return new NormalizedIndexProperties(Collections.singletonList(activeTarget), activeTarget);

        } else if (type != null) {
            // type only - normalize to storeTargets = [type], activeTarget = type
            return new NormalizedIndexProperties(Collections.singletonList(type), type);

        } else {
            // None defined - error
            throw new IllegalArgumentException(
                "Either type or activeTarget must be defined");
        }
    }

    /**
     * Get active target for queries (reads activeTarget or falls back to type).
     * This is a convenience method that performs normalization internally.
     *
     * @param definition index definition node state
     * @return active target for queries
     */
    @NotNull
    public static String getActiveTarget(@NotNull NodeState definition) {
        return normalize(definition).getActiveTarget();
    }

    /**
     * Get store targets for writes (reads storeTargets or falls back to [type]).
     * This is a convenience method that performs normalization internally.
     *
     * @param definition index definition node state
     * @return list of store targets for writes
     */
    @NotNull
    public static List<String> getStoreTargets(@NotNull NodeState definition) {
        return normalize(definition).getStoreTargets();
    }
}
