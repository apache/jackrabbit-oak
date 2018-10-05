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
package org.apache.jackrabbit.oak.spi.whiteboard;

import java.util.Map;

import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface Whiteboard {

    /**
     * Publishes the given service to the whiteboard. Use the returned
     * registration object to unregister the service.
     *
     * @param type type of the service
     * @param service service instance
     * @param properties service properties
     * @return service registration
     */
    <T> Registration register(Class<T> type, T service, Map<?, ?> properties);

    /**
     * Starts tracking services of the given type.
     *
     * @param type type of the services to track
     * @return service tracker
     */
    <T> Tracker<T> track(Class<T> type);

    /**
     * Starts tracking services of the given type, with given attributes.
     *
     * @param type type of the services to track
     * @param filterProperties only services with these properties will be tracked.
     *                         Null keys are not permitted. Null values means that
     *                         the property should be absent.
     * @return service tracker
     */
    <T> Tracker<T> track(Class<T> type, Map<String, String> filterProperties);

}
