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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream;

import java.io.Closeable;
import java.io.IOException;

/**
 * A reader for node data.
 */
public interface NodeDataReader extends Closeable {

    /**
     * Read the next node.
     *
     * @return the node, or null for EOF
     * @throws IOException
     */
    NodeData readNode() throws IOException;

    /**
     * Get the file size.
     *
     * @return the file size
     */
    long getFileSize();

    /**
     * Get the progress in percent (0..100).
     *
     * @return the progress
     */
    int getProgressPercent();
}
