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

package org.apache.jackrabbit.oak.spi.blob;

import org.apache.jackrabbit.core.data.DataStore;

import java.util.Map;

/**
 * Provides a DataStore instance for specific role. A role indicates what type
 * of data store it is.
 */
public interface DataStoreProvider {
    /**
     * Service property name which determine what role this DataStore is playing.
     */
    String ROLE = "role";
    String READ_ONLY = "readOnly";

    DataStore getDataStore();
    String getRole();
    Map<String, Object> getConfig();
}
