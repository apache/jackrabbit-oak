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
package org.apache.jackrabbit.oak.stats;

import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Marker interface for monitors that are to be registered with a
 * {@link org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard}.
 */
@ProviderType
public interface Monitor<T> {

    /**
     * @return The type to be passed to
     * {@link org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard#register(Class, Object, Map)}
     */
    @NotNull
    Class<T> getMonitorClass();

    /**
     * @return The properties to be passed to
     * {@link org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard#register(Class, Object, Map)}
     */
    @NotNull
    Map<Object, Object> getMonitorProperties();
}
