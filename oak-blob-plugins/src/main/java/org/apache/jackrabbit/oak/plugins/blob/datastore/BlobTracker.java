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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Track the blob ids.
 */
public interface BlobTracker extends Closeable {
    /**
     * Adds the given id.
     *
     * @param id the record id to be tracked
     * @throws IOException
     */
    void add(String id) throws IOException;

    /**
     * Adds the given ids.
     *
     * @param recs
     * @throws IOException
     */
    void add(Iterator<String> recs) throws IOException;

    /**
     * Adds the ids in the given file.
     *
     * @param recs
     * @throws IOException
     */
    void add(File recs) throws IOException;

    /**
     * Remove the given ids.
     *
     * @param recs
     * @throws IOException
     */
    void remove(Iterator<String> recs) throws IOException;

    /**
     * Remove the ids in the given file and deletes the file.
     *
     * @param recs
     * @throws IOException
     */
    void remove(File recs) throws IOException;

    /**
     * Remove the ids in the given file and deletes the file.
     *
     * @param recs
     * @throws IOException
     */
    void remove(File recs, Options options) throws IOException;
    /**
     * Fetches an iterator of records available.
     *
     * @return
     * @throws IOException
     */
    Iterator<String> get() throws IOException;

    /**
     * Fetches a File object which having all the sorted records.
     * The lifecycle of the returned {@link File} handle is the responsibility of the handler.
     *
     * @return
     * @throws IOException
     */
    File get(String path) throws IOException;

    enum Options {DEFAULT, ACTIVE_DELETION}
}
