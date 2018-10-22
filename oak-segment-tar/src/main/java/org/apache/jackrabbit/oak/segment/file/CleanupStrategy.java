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

package org.apache.jackrabbit.oak.segment.file;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;

interface CleanupStrategy {

    interface Context {

        GCListener getGCListener();

        SegmentCache getSegmentCache();

        SegmentTracker getSegmentTracker();

        FileStoreStats getFileStoreStats();

        GCNodeWriteMonitor getCompactionMonitor();

        GCJournal getGCJournal();

        Predicate<GCGeneration> getReclaimer();

        TarFiles getTarFiles();

        Revisions getRevisions();

        String getCompactedRootId();

        String getSegmentEvictionReason();

    }

    List<String> cleanup(Context context) throws IOException;

}
