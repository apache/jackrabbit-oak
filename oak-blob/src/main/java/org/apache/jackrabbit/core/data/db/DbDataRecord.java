/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.data.db;

import org.apache.jackrabbit.core.data.AbstractDataRecord;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;

import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * Data record that is stored in a database
 */
public class DbDataRecord extends AbstractDataRecord {

    protected final DbDataStore store;
    protected final long length;
    protected long lastModified;

    /**
     * Creates a data record based on the given identifier and length.
     *
     * @param identifier data identifier
     * @param length the length
     * @param lastModified
     */
    public DbDataRecord(DbDataStore store, DataIdentifier identifier, long length, long lastModified) {
        super(store, identifier);
        this.store = store;
        this.length = length;
        this.lastModified = lastModified;
    }

    /**
     * {@inheritDoc}
     */
    public long getLength() throws DataStoreException {
        lastModified = store.touch(getIdentifier(), lastModified);
        return length;
    }

    /**
     * {@inheritDoc}
     */
    public InputStream getStream() throws DataStoreException {
        lastModified = store.touch(getIdentifier(), lastModified);
        return new BufferedInputStream(new DbInputStream(store, getIdentifier()));
    }

    /**
     * {@inheritDoc}
     */
    public long getLastModified() {
        return lastModified;
    }
}
