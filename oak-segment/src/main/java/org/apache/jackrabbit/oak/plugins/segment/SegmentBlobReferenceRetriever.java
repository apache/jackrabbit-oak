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
package org.apache.jackrabbit.oak.plugins.segment;

import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;

/**
 * Implementation of {@link BlobReferenceRetriever} to retrieve blob references from the
 * {@link SegmentTracker}.
 */
@Deprecated
public class SegmentBlobReferenceRetriever implements BlobReferenceRetriever {

    private final SegmentTracker tracker;

    @Deprecated
    public SegmentBlobReferenceRetriever(SegmentTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    @Deprecated
    public void collectReferences(final ReferenceCollector collector) {
        tracker.collectBlobReferences(collector);
    }
}

