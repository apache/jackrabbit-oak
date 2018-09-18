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

import java.io.InputStream;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class SegmentBlob implements Blob {

    private final Backend backend;

    private final String segmentId;

    private final Segment segment;

    SegmentBlob(Backend backend, String segmentId, Segment segment) {
        this.backend = backend;
        this.segmentId = segmentId;
        this.segment = segment;
    }

    @NotNull
    @Override
    public InputStream getNewStream() {
        return backend.getSegmentData(segmentId)
            .orElseThrow(() -> new IllegalStateException("segment not found"));
    }

    @Override
    public long length() {
        return segment.getLength();
    }

    @Nullable
    @Override
    public String getReference() {
        return null;
    }

    @Nullable
    @Override
    public String getContentIdentity() {
        return null;
    }

}
