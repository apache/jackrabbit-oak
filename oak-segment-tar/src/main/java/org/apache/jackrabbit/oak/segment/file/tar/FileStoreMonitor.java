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

package org.apache.jackrabbit.oak.segment.file.tar;

/**
 * FileStoreMonitor are notified for any writes or deletes
 * performed by FileStore
 */
public interface FileStoreMonitor {

    /**
     * Notifies the monitor when data is written
     *
     * @param bytes number of bytes written
     */
    void written(long bytes);

    /**
     * Notifies the monitor when memory is reclaimed
     *
     * @param bytes number of bytes reclaimed
     */
    void reclaimed(long bytes);
    
    /**
     * Notifies the monitor when journal data is flushed to disk.
     */
    void flushed();

}
