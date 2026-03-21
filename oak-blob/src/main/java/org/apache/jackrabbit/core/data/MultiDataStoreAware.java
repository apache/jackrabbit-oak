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
package org.apache.jackrabbit.core.data;

import org.apache.jackrabbit.core.data.MultiDataStore.MoveDataTask;

/**
 * To use a DataStore within a MultiDataStore it must implement this
 * MultiDataStoreAware Interface. It extends a DataStore to delete a single
 * DataRecord.
 */
public interface MultiDataStoreAware {

    /**
     * Deletes a single DataRecord based on the given identifier. Delete will
     * only be used by the {@link MoveDataTask}.
     * 
     * @param identifier
     *            data identifier
     * @throws DataStoreException
     *             if the data store could not be accessed, or if the given
     *             identifier is invalid
     */
    void deleteRecord(DataIdentifier identifier) throws DataStoreException;

}
