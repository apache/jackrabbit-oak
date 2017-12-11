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
package org.apache.jackrabbit.oak.run.cli;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.jackrabbit.core.data.AbstractDataRecord;
import org.apache.jackrabbit.core.data.AbstractDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;

/**
 * DataStore implementation which creates empty files matching given identifier.
 *
 * This can be use to try migration of repositories where DataStore size is large
 * and cannot be accessed as transferring them would take quite a bit of time. As migration
 * does not involve accessing the actual binary content and only binary identifiers are
 * transferred it should enable us to get past the migration phase
 */
public class DummyDataStore extends OakFileDataStore {

    public DummyDataStore() {
        //Set min size to match the one in ClusterDataStore
        setMinRecordLength(4096);
    }

    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        DataRecord dr = super.getRecordIfStored(identifier);
        if (dr == null) {
            dr = new DummyDataRecord(this, identifier);
        }
        return dr;
    }

    private static final class DummyDataRecord extends AbstractDataRecord {

        private DummyDataRecord(AbstractDataStore store, DataIdentifier identifier) {
            super(store, identifier);
        }

        public long getLength() throws DataStoreException {
            return 0;
        }

        public InputStream getStream() throws DataStoreException {
            return new ByteArrayInputStream(new byte[0]);
        }

        public long getLastModified() {
            return 0;
        }
    }
}
