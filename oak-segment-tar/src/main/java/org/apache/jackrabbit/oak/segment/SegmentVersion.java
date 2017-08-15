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

package org.apache.jackrabbit.oak.segment;

import static java.util.Collections.max;
import static java.util.EnumSet.allOf;

import com.google.common.primitives.UnsignedBytes;

/**
 * Version of the segment storage format. <ul> <li>12 = all oak-segment-tar
 * versions</li> </ul>
 */
public enum SegmentVersion {

    /*
     * ON OLDER VERSIONS
     *
     * The legacy Segment Store implemented in oak-segment makes use of version
     * numbers 10 and 11. These version numbers identify two variations of the
     * data format that oak-segment can parse and understand.
     *
     * For oak-segment-tar 10 and 11 are invalid values for the segment version.
     * The data format identified by these versions is not understood by
     * oak-segment-tar. No special handling is needed for versions 10 and 11.
     * From the perspective of oak-segment-tar, they are just invalid. The first
     * valid version for oak-segment-tar is 12.
     *
     * As a consequence, if you find yourself debugging code from
     * oak-segment-tar and you detect that version 10 or 11 is used in some
     * segment, you are probably trying to read the old data format with the new
     * code.
     */

    V_12((byte) 12),
    V_13((byte) 13);

    /**
     * Latest segment version
     */
    public static final SegmentVersion LATEST_VERSION = max(allOf(SegmentVersion.class),
        (v1, v2) -> UnsignedBytes.compare(v1.version, v2.version));

    private final byte version;

    SegmentVersion(byte version) {
        this.version = version;
    }

    public static byte asByte(SegmentVersion v) {
        return v.version;
    }

    public static SegmentVersion fromByte(byte v) {
        if (v == V_13.version) {
            return V_13;
        }
        if (v == V_12.version) {
            return V_12;
        }
        throw new IllegalArgumentException("Unknown version " + v);
    }

    public static boolean isValid(byte v) {
        return v == V_13.version || v == V_12.version;
    }

    public static boolean isValid(SegmentVersion version) {
        return isValid(version.version);
    }

}
