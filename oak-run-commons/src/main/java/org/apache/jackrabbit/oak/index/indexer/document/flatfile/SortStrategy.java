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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;

import java.io.File;
import java.io.IOException;

/**
 * @deprecated depending on what type of store it is use {@link org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreSortStrategy}  or {@link org.apache.jackrabbit.oak.index.indexer.document.incrementalstore.IncrementalIndexStoreSortStrategy} instead
 */
@Deprecated
public interface SortStrategy {

    File createSortedStoreFile() throws IOException, CompositeException;

    long getEntryCount();
}
