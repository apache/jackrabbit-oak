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

package org.apache.jackrabbit.oak.plugins.index.importer;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;

/**
 * Coordinates the switching of indexing lane for indexes which are
 * to be imported. Its support idempotent operation i.e. if an
 * indexer is switched to temp lane then a repeat of same
 * operation would be no op.
 */
public class AsyncLaneSwitcher {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncLaneSwitcher.class);

    /**
     * Property name where previous value of 'async' is stored
     */
    static final String ASYNC_PREVIOUS = "async-previous";

    /**
     * Value stored in previous async property if the index is not async
     * i.e. when a sync index is reindexed in out of band mode
     */
    static final String ASYNC_PREVIOUS_NONE = "none";

    /**
     * Index lane name which is used for indexing
     */
    private static final String TEMP_LANE_PREFIX = "temp-";

    /**
     * Make a copy of current async value and replace it with one required for offline reindexing.
     * The switch lane operation can be safely repeated: if the index lane is already set to the
     * target lane, the call is a no-op.
     * <p>
     * If {@code async-previous} is present but {@code async} does not already equal
     * {@code laneName}, the property is treated as stale (e.g. copied from a running system
     * into a user-provided index definition). In that case the stale value is discarded and
     * the switch proceeds normally so the index is not silently skipped during reindexing.
     * If {@code async-previous} is missing, then the switch to the temporary lane required for
     * offline reindexing has not yet happened and will be carried out.
     */
    public static void switchLane(NodeBuilder idxBuilder, String laneName) {
        PropertyState currentAsyncState = idxBuilder.getProperty(ASYNC_PROPERTY_NAME);
        PropertyState newAsyncState = PropertyStates.createProperty(ASYNC_PROPERTY_NAME, laneName, Type.STRING);

        if (idxBuilder.hasProperty(ASYNC_PREVIOUS)) {
            if (currentAsyncState != null
                    && !currentAsyncState.isArray()
                    && laneName.equals(currentAsyncState.getValue(Type.STRING))) {
                // Lane already switched to the target — no-op
                return;
            }
            // async-previous is present but async != target lane: stale / user-provided value
            // Discard it and fall through to perform the switch correctly
            LOG.warn("Index definition contains stale '{}' property (async='{}', target lane='{}') - " +
                    "discarding stale value and switching lane", ASYNC_PREVIOUS, currentAsyncState, laneName);
            idxBuilder.removeProperty(ASYNC_PREVIOUS);
        }

        PropertyState previousAsyncState;
        if (currentAsyncState == null) {
            previousAsyncState = PropertyStates.createProperty(ASYNC_PREVIOUS, ASYNC_PREVIOUS_NONE);
        } else {
            //Ensure that previous state is copied with correct type
            previousAsyncState = clone(ASYNC_PREVIOUS, currentAsyncState);
        }

        idxBuilder.setProperty(previousAsyncState);
        idxBuilder.setProperty(newAsyncState);
    }

    public static boolean isLaneSwitched(NodeBuilder idxBuilder) {
        return idxBuilder.hasProperty(ASYNC_PREVIOUS);
    }

    public static String getTempLaneName(String laneName){
        return TEMP_LANE_PREFIX + laneName;
    }

    public static void revertSwitch(NodeBuilder idxBuilder, String indexPath) {
        PropertyState previousAsync = idxBuilder.getProperty(ASYNC_PREVIOUS);
        Validate.checkState(previousAsync != null, "No previous async state property found for index [%s]", indexPath);

        if (isNone(previousAsync)) {
            idxBuilder.removeProperty(IndexConstants.ASYNC_PROPERTY_NAME);
        } else {
            idxBuilder.setProperty(clone(IndexConstants.ASYNC_PROPERTY_NAME, previousAsync));
        }
        idxBuilder.removeProperty(ASYNC_PREVIOUS);
        // Set the refresh flag to true here otherwise the lane changes won't reflect in the storedIndexDefinition.
        idxBuilder.setProperty(IndexConstants.REFRESH_PROPERTY_NAME, true);
    }

    public static boolean isNone(PropertyState previousAsync) {
        return !previousAsync.isArray() && ASYNC_PREVIOUS_NONE.equals(previousAsync.getValue(Type.STRING));
    }

    private static PropertyState clone(String newName, PropertyState currentAsyncState) {
        PropertyState clonedState;
        if (currentAsyncState.isArray()) {
            clonedState = PropertyStates.createProperty(newName, currentAsyncState.getValue(Type.STRINGS), Type.STRINGS);
        } else {
            clonedState = PropertyStates.createProperty(newName, currentAsyncState.getValue(Type.STRING), Type.STRING);
        }
        return clonedState;
    }
}
