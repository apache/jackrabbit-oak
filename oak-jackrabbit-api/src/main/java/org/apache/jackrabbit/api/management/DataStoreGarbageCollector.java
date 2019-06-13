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
 * Garbage collector for DataStore. This implementation iterates through all
 * nodes and reads the binary properties. To detect nodes that are moved while
 * the scan runs, event listeners are started. Like the well known garbage
 * collection in Java, the items that are still in use are marked. Currently
 * this is achieved by updating the modified date of the entries. Newly added
 * entries are detected because the modified date is changed when they are
 * added.
 * <p>
 * Example code to run the data store garbage collection:
 * <pre>
 * JackrabbitRepositoryFactory jf = (JackrabbitRepositoryFactory) factory;
 * RepositoryManager m = jf.getRepositoryManager((JackrabbitRepository) repository);
 * GarbageCollector gc = m.createDataStoreGarbageCollector();
 * gc.mark();
 * gc.sweep();
 * </pre>
 */
public interface DataStoreGarbageCollector {

    /**
     * Set the delay between scanning items.
     * The main scan loop sleeps this many milliseconds after
     * scanning a node. The default is 0, meaning the scan should run at full speed.
     *
     * @param millis the number of milliseconds to sleep
     */
    void setSleepBetweenNodes(long millis);

    /**
     * Get the delay between scanning items.
     *
     * @return the number of milliseconds to sleep
     */
    long getSleepBetweenNodes();

    /**
     * Set the event listener. If set, the event listener will be called
     * for each item that is scanned. This mechanism can be used
     * to display the progress.
     *
     * @param callback if set, this is called while scanning
     */
    void setMarkEventListener(MarkEventListener callback);

    /**
     * Enable or disable using the IterablePersistenceManager interface
     * to scan the items. This is important for clients that need
     * the complete Node implementation in the ScanEventListener
     * callback.
     *
     * @param allow true if using the IterablePersistenceManager interface is allowed
     */
    void setPersistenceManagerScan(boolean allow);

    /**
     * Check if using the IterablePersistenceManager interface is allowed.
     *
     * @return true if using IterablePersistenceManager is possible.
     */
    boolean isPersistenceManagerScan();

    /**
     * Scan the repository. The garbage collector will iterate over all nodes in the repository
     * and update the last modified date. If all persistence managers implement the
     * IterablePersistenceManager interface, this mechanism is used; if not, the garbage
     * collector scans the repository using the JCR API starting from the root node.
     *
     * @throws RepositoryException
     */
    void mark() throws RepositoryException;

    /**
     * Delete all unused items in the data store.
     *
     * @return the number of deleted items
     * @throws RepositoryException
     */
    int sweep() throws RepositoryException;

    /**
     * Cleanup resources used internally by this instance.
     */
    void close();

}
