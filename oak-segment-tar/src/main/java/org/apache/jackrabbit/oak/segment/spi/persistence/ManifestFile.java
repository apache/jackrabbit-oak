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
package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;
import java.util.Properties;

/**
 * Manifest is a properties files, providing the information about the segment
 * store (eg. the schema version number).
 * <p>
 * The implementation <b>doesn't need to be</b> thread-safe.
 */
public interface ManifestFile {

    /**
     * Check if the manifest already exists.
     * @return {@code true} if the manifest exists
     */
    boolean exists();

    /**
     * Load the properties from the manifest file.
     * @return properties describing the segmentstore
     * @throws IOException
     */
    Properties load() throws IOException;

    /**
     * Store the properties to the manifest file.
     * @param properties describing the segmentstore
     * @throws IOException
     */
    void save(Properties properties) throws IOException;

}
