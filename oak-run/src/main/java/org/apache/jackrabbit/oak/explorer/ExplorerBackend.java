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

package org.apache.jackrabbit.oak.explorer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

interface ExplorerBackend {

    void open() throws IOException;

    void close();

    List<String> readRevisions();

    Map<String, Set<UUID>> getTarReaderIndex();

    Map<UUID, Set<UUID>> getTarGraph(String file) throws IOException;

    List<String> getTarFiles();

    void getGcRoots(UUID uuidIn, Map<UUID, Set<Entry<UUID, String>>> links) throws IOException;

    Set<UUID> getReferencedSegmentIds();

    NodeState getHead();

    NodeState readNodeState(String recordId);

    void setRevision(String revision);

    boolean isPersisted(NodeState state);

    boolean isPersisted(PropertyState state);

    String getRecordId(NodeState state);

    UUID getSegmentId(NodeState state);

    String getRecordId(PropertyState state);

    UUID getSegmentId(PropertyState state);

    String getTemplateRecordId(NodeState state);

    UUID getTemplateSegmentId(NodeState state);

    String getFile(NodeState state);

    String getFile(PropertyState state);

    String getTemplateFile(NodeState state);

    Map<UUID, String> getBulkSegmentIds(Blob blob);

    String getPersistedCompactionMapStats();

    boolean isExternal(Blob blob);
}
