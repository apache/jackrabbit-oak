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
package org.apache.jackrabbit.oak.plugins.document;

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * During hardening of FullGC one can choose level type of garbage should be cleaned up.
 * Ultimately the goal is to clean up all possible garbage. After hardening these modes
 * might no longer be supported.
 */
public enum FullGCMode {
    /**
     * no full GC is done at all
     */
    NONE,
    /**
     * GC only empty properties
     */
    EMPTYPROPS,
    /**
     * GC only orphaned nodes with gaps in ancestor docs
     */
    GAP_ORPHANS,
    /**
     * GC orphaned nodes with gaps in ancestor docs, plus empty properties
     */
    GAP_ORPHANS_EMPTYPROPS,
    /**
     * GC any kind of orphaned nodes, plus empty properties
     */
    ALL_ORPHANS_EMPTYPROPS,
    /**
     * GC any kind of orphaned nodes, empty properties plus keep 1 (== keep
     * traversed) revision, applied to user properties only
     */
    ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS,
    /**
     * GC any kind of orphaned nodes, empty properties plus keep 1 (== keep
     * traversed) revision, applied to all properties
     */
    ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS,
    /**
     * GC any kind of orphaned nodes, empty properties plus cleanup unmerged BCs
     */
    ORPHANS_EMPTYPROPS_UNMERGED_BC,
    /**
     * GC any kind of orphaned nodes, empty properties plus cleanup revisions, also
     * between checkpoints
     */
    ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC,
    /**
     * GC any kind of orphaned nodes, empty properties, cleanup revisions, also
     * between checkpoints, plus cleanup unmerged BCs
     */
    ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC;

    private static final Logger log = getLogger(FullGCMode.class);

    public static FullGCMode getMode(final int mode) {

        switch (mode) {
            case 0:
                return NONE;
            case 1:
                return EMPTYPROPS;
            case 2:
                return GAP_ORPHANS;
            case 3:
                return GAP_ORPHANS_EMPTYPROPS;
            case 4:
                return ALL_ORPHANS_EMPTYPROPS;
            case 5:
                return ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS;
            case 6:
                return ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS;
            case 7:
                return ORPHANS_EMPTYPROPS_UNMERGED_BC;
            case 8:
                return ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC;
            case 9:
                return ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC;
            default:
                log.warn("Unsupported full GC mode configuration value: {}. Resetting to NONE", mode);
                return NONE;
        }
    }

}
