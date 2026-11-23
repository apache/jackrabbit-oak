/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

/**
 * Notified when a blob is deleted from an index file.
 * Allows the blob store GC to track which blobs are no longer referenced
 * so they can be reclaimed without waiting for the next full GC scan.
 *
 * @see org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.BlobDeletionCallback
 */
@FunctionalInterface
public interface BlobDeletionCallback {

    BlobDeletionCallback NOOP = (blobId, path) -> {};

    /**
     * Called for each blob whose reference is removed when an index file is deleted.
     *
     * @param blobId content identity of the deleted blob
     * @param path   context path [indexPath, storageNodeName, fileName] for diagnostics
     */
    void deleted(String blobId, Iterable<String> path);
}
