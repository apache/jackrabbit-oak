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

package org.apache.jackrabbit.oak.blob.composite.delegate;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class IntelligentDelegateHandler implements DelegateHandler {
    private static Logger LOG = LoggerFactory.getLogger(IntelligentDelegateHandler.class);

    private Map<String, DataStore> dataStoresByBundleName = Maps.newConcurrentMap();

    private List<DataStore> nonFilteredWritableDataStores = Lists.newArrayList();
    private List<DataStore> nonFilteredReadOnlyDataStores = Lists.newArrayList();

    @Reference
    private DelegateMinRecordLengthSelector minRecordLengthSelector;

    @Override
    public String toString() {
        return String.format("Strategy: %s, data store bundles: %s, writable data stores: %s, readonly data stores: %s, rec len chooser: %s",
                this.getClass().getSimpleName(),
                dataStoresByBundleName,
                nonFilteredWritableDataStores,
                nonFilteredReadOnlyDataStores,
                minRecordLengthSelector.getClass().getSimpleName());
    }

    @Override
    public void addDelegateDataStore(final DelegateDataStore ds) {
//        if (spec.isReadOnly()) {
//            nonFilteredReadOnlyDataStores.add(ds);
//        }
//        else {
//            nonFilteredWritableDataStores.add(ds);
//        }
//        dataStoresByBundleName.put(spec.getBundleName(), ds);
    }

    @Override
    public void removeDelegateDataStoresForBundle(Bundle bundle) {
        String bundleName = bundle.getSymbolicName();
        if (dataStoresByBundleName.containsKey(bundleName)) {
            DataStore toRemove = dataStoresByBundleName.get(bundleName);
            for (List<DataStore> l : Lists.newArrayList(nonFilteredWritableDataStores,
                    nonFilteredReadOnlyDataStores)) {
                l.remove(toRemove);
            }
        }
    }

    @Override
    public boolean hasDelegate() {
        return (! nonFilteredWritableDataStores.isEmpty() || ! nonFilteredReadOnlyDataStores.isEmpty());
    }

    @Override
    public DataStore selectWritableDelegate(final DataIdentifier identifier) {
        Iterator<DataStore> iter = getDelegateIterator(identifier, DelegateTraversalOptions.RW_ONLY);
        DataStore firstWritableDelegate = null;
        while (iter.hasNext()) {
            DataStore writableDelegate = iter.next();
            if (null == firstWritableDelegate) {
                firstWritableDelegate = writableDelegate;
            }
            try {
                if (null != writableDelegate.getRecordIfStored(identifier)) {
                    return writableDelegate;
                }
            }
            catch (DataStoreException dse) {
                LOG.warn("Unable to access delegate data store while selecting writable delegate", dse);
            }
        }
        if (null == firstWritableDelegate) {
            LOG.error("No writable delegates configured - could not select a writable delegate");
        }
        return firstWritableDelegate;
    }

    @Override
    public Iterator<DataStore> getDelegateIterator() {
        return getIterator(Optional.empty(), Optional.empty());
    }

    @Override
    public Iterator<DataStore> getDelegateIterator(final DataIdentifier identifier) {
        return getIterator(Optional.of(identifier), Optional.empty());
    }

    @Override
    public Iterator<DataStore> getDelegateIterator(final DelegateTraversalOptions options) {
        return getIterator(Optional.empty(), Optional.of(options));
    }

    @Override
    public Iterator<DataStore> getDelegateIterator(final DataIdentifier identifier, final DelegateTraversalOptions options) {
        return getIterator(Optional.of(identifier), Optional.of(options));
    }

    private Iterator<DataStore> getIterator(final Optional<DataIdentifier> identifierOptional, final Optional<DelegateTraversalOptions> optionsOptional) {
        // DataIdentifiers and DelegateTraversalOptions are used to limit the number of data stores we iterate.
        // If these are not provided we return all iterators.

        if (optionsOptional.isPresent()) {
            DelegateTraversalOptions options = optionsOptional.get();
            Iterator<DataStore> i;
            if (DelegateTraversalOptions.RORW_OPTION.RO_ONLY == options.readWriteOption) {
                i = nonFilteredReadOnlyDataStores.iterator();
            }
            else if (DelegateTraversalOptions.RORW_OPTION.RW_ONLY == options.readWriteOption) {
                i = nonFilteredWritableDataStores.iterator();
            }
            else {
                i = Iterators.concat(nonFilteredWritableDataStores.iterator(),
                        nonFilteredReadOnlyDataStores.iterator());
            }

            return i;
        }
        else {
            return Iterators.concat(
                    nonFilteredWritableDataStores.iterator(),
                    nonFilteredReadOnlyDataStores.iterator()
            );
        }
    }

    @Override
    public int getMinRecordLength() {
        return minRecordLengthSelector.getMinRecordLength(this);
    }
}