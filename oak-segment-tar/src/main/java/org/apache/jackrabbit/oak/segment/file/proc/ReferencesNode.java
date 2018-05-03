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

package org.apache.jackrabbit.oak.segment.file.proc;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;

class ReferencesNode extends AbstractNode {

    private final Backend backend;

    private final String segmentId;

    ReferencesNode(Backend backend, String segmentId) {
        this.backend = backend;
        this.segmentId = segmentId;
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return backend.getSegmentReferences(segmentId)
            .map(this::getChildNodeEntries)
            .orElse(Collections.emptyList());
    }

    private Iterable<ChildNodeEntry> getChildNodeEntries(Iterable<String> references) {
        return StreamSupport.stream(references.spliterator(), false)
            .map(this::newSegmentNodeEntry)
            .collect(Collectors.toList());
    }

    private ChildNodeEntry newSegmentNodeEntry(String segmentId) {
        return new SegmentEntry(backend, segmentId);
    }

}
