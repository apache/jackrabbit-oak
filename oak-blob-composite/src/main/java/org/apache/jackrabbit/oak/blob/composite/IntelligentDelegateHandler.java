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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.Component;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class IntelligentDelegateHandler implements DelegateHandler {
    private static Logger LOG = LoggerFactory.getLogger(IntelligentDelegateHandler.class);

    private Map<DataStoreProvider, Set<DataIdentifier>> handledProviders = Maps.newConcurrentMap();
    private List<DataStore> nonFilteredWritableDataStores = Lists.newArrayList();
    private List<DataStore> nonFilteredReadOnlyDataStores = Lists.newArrayList();

    //@Reference
    private DelegateMinRecordLengthSelector minRecordLengthSelector = new GuaranteedMinRecordLengthSelector();

    @Override
    public String toString() {
        return String.format("Strategy: %s, writable data stores: %s, readonly data stores: %s, rec len chooser: %s",
                this.getClass().getSimpleName(),
                nonFilteredWritableDataStores,
                nonFilteredReadOnlyDataStores,
                minRecordLengthSelector.getClass().getSimpleName());
    }

    @Override
    public void addDelegateDataStore(final DelegateDataStore ds) {
        if (! handledProviders.containsKey(ds.getDataStore())) {
            handledProviders.put(ds.getDataStore(), Sets.newConcurrentHashSet());
            if (ds.isReadOnly()) {
                nonFilteredReadOnlyDataStores.add(ds.getDataStore().getDataStore());
            } else {
                nonFilteredWritableDataStores.add(ds.getDataStore().getDataStore());
            }
        }
    }

    @Override
    public boolean removeDelegateDataStore(final DataStoreProvider provider) {
        boolean wasProviderRemoved = false;
        if (handledProviders.containsKey(provider)) {
            for (List<DataStore> l : Lists.newArrayList(nonFilteredWritableDataStores,
                    nonFilteredReadOnlyDataStores)) {
                wasProviderRemoved |= l.remove(provider.getDataStore());
            }
            if (wasProviderRemoved) {
                handledProviders.remove(provider);
            }
        }
        return wasProviderRemoved;
    }

    @Override
    public boolean hasDelegate() {
        return (! nonFilteredWritableDataStores.isEmpty() || ! nonFilteredReadOnlyDataStores.isEmpty());
    }

    @Override
    public void mapIdentifierToDelegate(final DataIdentifier identifier, final DataStore delegate) {
        for (Map.Entry<DataStoreProvider, Set<DataIdentifier>> entry : handledProviders.entrySet()) {
            if (entry.getKey().getDataStore().equals(delegate)) {
                entry.getValue().add(identifier);
            }
        }
    }

    @Override
    public void unmapIdentifierFromDelegates(final DataIdentifier identifier) {
        for (Set<DataIdentifier> idSet : handledProviders.values()) {
            idSet.remove(identifier);
        }
    }

    private boolean delegateProbablyHandlesIdentifier(final DataStore delegate, final DataIdentifier identifier) {
        for (Map.Entry<DataStoreProvider, Set<DataIdentifier>> entry : handledProviders.entrySet()) {
            if (entry.getKey().getDataStore().equals(delegate)
                    && entry.getValue().contains(identifier)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<DataStore> getWritableDelegatesIterator() {
        return getIterator(null, true);
    }

    @Override
    public Iterator<DataStore> getWritableDelegatesIterator(@Nullable final DataIdentifier identifier) {
        return getIterator(identifier, true);
    }

    @Override
    public Iterator<DataStore> getAllDelegatesIterator() {
        return getIterator(null, false);
    }

    @Override
    public Iterator<DataStore> getAllDelegatesIterator(final DataIdentifier identifier) {
        return getIterator(identifier, false);
    }

    private Iterator<DataStore> getIterator(@Nullable final DataIdentifier identifier, final boolean writableOnly) {
        if (null != identifier) {
            List<DataStore> matchingDataStores = Lists.newArrayList();
            for (DataStore ds : nonFilteredWritableDataStores) {
                if (delegateProbablyHandlesIdentifier(ds, identifier)) {
                    matchingDataStores.add(ds);
                }
            }
            if (! writableOnly) {
                for (DataStore ds : nonFilteredReadOnlyDataStores) {
                    if (delegateProbablyHandlesIdentifier(ds, identifier)) {
                        matchingDataStores.add(ds);
                    }
                }
            }
            return matchingDataStores.iterator();
        }

        // If no identifier provided, return iterator to all applicable delegates
        return writableOnly ? nonFilteredWritableDataStores.iterator() :
                Iterators.concat(nonFilteredWritableDataStores.iterator(), nonFilteredReadOnlyDataStores.iterator());
    }

    @Override
    public int getMinRecordLength() {
        return minRecordLengthSelector.getMinRecordLength(this);
    }
}