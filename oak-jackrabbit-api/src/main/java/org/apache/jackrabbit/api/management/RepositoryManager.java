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
package org.apache.jackrabbit.api.management;

import javax.jcr.RepositoryException;

/**
 * The repository manager provides life-cycle management features for
 * repositories.
 *
 * Not all implementations are required to implement all features,
 * for example some implementations may not support starting a repository after
 * is has been stopped.
 */
public interface RepositoryManager {

    /**
     * Shuts down the repository. A Jackrabbit repository instance contains
     * a acquired resources and cached data that needs to be released and
     * persisted when the repository is no longer used. This method handles
     * all these shutdown tasks and <em>should</em> therefore be called by the
     * client application once the repository instance is no longer used.
     * <p>
     * Possible errors are logged rather than thrown as exceptions as there
     * is little that a client application could do in such a case.
     */
    void stop();

    /**
     * Create a data store garbage collector for this repository.
     *
     * @return the data store garbage collector if the data store is enabled, null otherwise
     */
    DataStoreGarbageCollector createDataStoreGarbageCollector() throws RepositoryException;

}
