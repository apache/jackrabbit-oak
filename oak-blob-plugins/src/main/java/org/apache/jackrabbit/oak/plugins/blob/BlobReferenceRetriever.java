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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.IOException;

import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Interface to abstract out the low-level details of retrieving blob references from different
 * {@link NodeStore}
 */
public interface BlobReferenceRetriever {

    /**
     * Collect references.
     * 
     * @param collector the collector to collect all references
     * @throws IOException
     */
    void collectReferences(ReferenceCollector collector) throws IOException;
}

