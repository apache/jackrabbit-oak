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

package org.apache.jackrabbit.oak.blob.composite;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;

import java.util.Iterator;

public interface DelegateHandler {
    /**
     * Add a {@link DelegateDataStore} to this handler.  This means the
     * specified delegate will be considered a delegate by this delegate handler.
     *
     * @param dataStore A {@link DelegateDataStore} to be handled by this handler
     */
    void addDelegateDataStore(final DelegateDataStore dataStore);

    /**
     * Remove a {@link DelegateDataStore} from this handler.  This means the
     * specified delegate will no longer be considered a delegate by this delegate
     * handler.
     *
     * @param provider
     * @return True if this action caused a {@link DataStoreProvider} to be removed,
     * False if no matching {@link DataStoreProvider} was found.
     */
    boolean removeDelegateDataStore(final DataStoreProvider provider);

    /**
     * Determine whether this handler has at least one delegate.
     *
     * @return true if the handler has at least one delegate
     */
    boolean hasDelegate();

    /**
     * Get a data store iterator for all writable delegates managed by the
     * {@link CompositeDataStore}.  They are returned in priority order based on the
     * handler's prioritization strategy.
     *
     * @return Iterator to writable data stores
     */
    Iterator<DataStore> getWritableDelegatesIterator();

    /**
     * Get a data store iterator for all writable delegates managed by the
     * {@link CompositeDataStore} that this handler believes contain a matching record
     * for the provided identifier.  The iterator may contain no data stores if
     * the handler believes the record doesn't exist in any writable delegate
     * data stores.
     *
     * @param identifier A {@link DataIdentifier} for filtering the data stores
     * @return Iterator to writable data stores
     */
    Iterator<DataStore> getWritableDelegatesIterator(final DataIdentifier identifier);

    /**
     * Get a data store iterator for all delegates managed by the
     * {@link CompositeDataStore}.  They are returned in priority order based on the
     * handler's prioritization strategy.
     *
     * @return Iterator to all data stores
     */
    Iterator<DataStore> getAllDelegatesIterator();

    /**
     * Get a data store iterator for all delegates managed by the
     * {@link CompositeDataStore} that this handler believes contain a matching record
     * for the provided identifier.  The iterator may contain no data stores if
     * the handler believes the record doesn't exist in any delegate data stores.
     *
     * @param identifier A {@link DataIdentifier} for filtering the data stores
     * @return Iterator to all data stores
     */
    Iterator<DataStore> getAllDelegatesIterator(final DataIdentifier identifier);

    /**
     * Returns the overall value for the minimum record length for all the managed
     * delegates, based on whatever strategy is employed by the injected
     * {@link DelegateMinRecordLengthSelector}.
     *
     * @return The value to use for the minimum record length
     */
    int getMinRecordLength();
}
