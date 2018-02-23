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
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

@Component
public class IntelligentDelegateHandler implements DelegateHandler {
    private static Logger LOG = LoggerFactory.getLogger(IntelligentDelegateHandler.class);

    // TODO:  Add mapping from blob IDs to DataStore (e.g. enhanced Bloom filter supporting deletes) -MR
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    private List<DataStore> nonFilteredWritableDataStores = Lists.newArrayList();
    private List<DataStore> nonFilteredReadOnlyDataStores = Lists.newArrayList();

    @Reference
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
        // TODO:  Add this data store to the blob ID mapper if not already there -MR
        // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
        DataStore delegate = ds.getDataStore().getDataStore();
        if (ds.isReadOnly()) {
            if (! nonFilteredReadOnlyDataStores.contains(delegate)) {
                nonFilteredReadOnlyDataStores.add(delegate);
            }
        } else {
            if (! nonFilteredWritableDataStores.contains(delegate)) {
                nonFilteredWritableDataStores.add(delegate);
            }
        }
    }

    @Override
    public boolean removeDelegateDataStore(final DataStoreProvider provider) {
        boolean wasProviderRemoved = false;
        for (List<DataStore> l : Lists.newArrayList(nonFilteredWritableDataStores,
                nonFilteredReadOnlyDataStores)) {
            wasProviderRemoved |= l.remove(provider.getDataStore());
        }
        // TODO:  Remove this data store from the blob ID mapper if exists -MR
        // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
        return wasProviderRemoved;
    }

    @Override
    public boolean hasDelegate() {
        return (! nonFilteredWritableDataStores.isEmpty() || ! nonFilteredReadOnlyDataStores.isEmpty());
    }

    @Override
    public void mapIdentifierToDelegate(final DataIdentifier identifier, final DataStore delegate) {
        // TODO:  Remember the mapping for this identifier to the data store -MR
        // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    }

    @Override
    public void unmapIdentifierFromDelegates(final DataIdentifier identifier) {
        // TODO:  Remove the mapping for this identifier from the data store -MR
        // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    }

    private boolean delegateProbablyHandlesIdentifier(final DataStore delegate, final DataIdentifier identifier) {
        // TODO:  Check to see if the blob ID mapper maps this identifier to the delegate -MR
        // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
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