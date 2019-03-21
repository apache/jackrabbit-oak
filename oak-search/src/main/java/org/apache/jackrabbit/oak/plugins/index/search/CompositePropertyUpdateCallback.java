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
package org.apache.jackrabbit.oak.plugins.index.search;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Composite {@link PropertyUpdateCallback}
 */
public class CompositePropertyUpdateCallback implements PropertyUpdateCallback{

    private final Collection<PropertyUpdateCallback> callbacks;

    public CompositePropertyUpdateCallback(@NotNull Collection<PropertyUpdateCallback> callbacks) {
        this.callbacks = callbacks;
        checkNotNull(callbacks);
    }

    @Override
    public void propertyUpdated(String nodePath, String propertyRelativePath, PropertyDefinition pd,
                                @Nullable PropertyState before, @Nullable PropertyState after) {
        for (PropertyUpdateCallback callback : callbacks) {
            callback.propertyUpdated(nodePath, propertyRelativePath, pd, before, after);
        }
    }

    @Override
    public void done() throws CommitFailedException {
        for (PropertyUpdateCallback callback : callbacks) {
            callback.done();
        }
    }
}
