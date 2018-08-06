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

package org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore;

import org.apache.jackrabbit.core.data.DataStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *  DataStore fixture for parametrized tests. To be used inside NodeStoreFixture implementations.
 */
public interface DataStoreFixture {

    /**
     * Create a new DataStore instance. This might include setting up a temporary folder,
     * bucket, container... of the underlying storage solution. Return null if this
     * DataStore is not available because of missing configuration or setup issues
     * (implementation shall log such warnings/errors).
     *
     * Calling DataStore.init() is left to the client.
     */
    @NotNull
    DataStore createDataStore();

    /**
     * Dispose a DataStore. This can include removing the temporary test folder, bucket etc.
     */
    void dispose(DataStore dataStore);

    /**
     * Return whether this fixture is available, for example if the necessary configuration is present.
     */
    boolean isAvailable();
}
