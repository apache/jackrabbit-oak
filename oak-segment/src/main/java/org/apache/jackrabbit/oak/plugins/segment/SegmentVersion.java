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

import static java.util.Collections.max;
import static java.util.EnumSet.allOf;

import java.util.Comparator;

import com.google.common.primitives.UnsignedBytes;

/**
 * Version of the segment storage format.
 * <ul>
 * <li>10 = all Oak versions previous to 11</li>
 * <li>11 = all Oak versions starting from 1.0.12, 1.1.7 and 1.2</li>
 * </ul>
 */
@Deprecated
public enum SegmentVersion {

    /**
     * @deprecated Use latest version V11
     */
    @Deprecated
    V_10((byte) 10),

    @Deprecated
    V_11((byte) 11);

    /**
     * Latest segment version
     */
    @Deprecated
    public static SegmentVersion LATEST_VERSION = max(allOf(SegmentVersion.class),
        new Comparator<SegmentVersion>() {
            @Override
            public int compare(SegmentVersion v1, SegmentVersion v2) {
                return UnsignedBytes.compare(v1.version, v2.version);
            }
    });

    private final byte version;

    SegmentVersion(byte version) {
        this.version = version;
    }

    @Deprecated
    public boolean onOrAfter(SegmentVersion other) {
        return compareTo(other) >= 0;
    }

    @Deprecated
    public static byte asByte(SegmentVersion v) {
        return v.version;
    }

    @Deprecated
    public static SegmentVersion fromByte(byte v) {
        if (v == V_11.version) {
            return V_11;
        } else if (v == V_10.version) {
            return V_10;
        } else {
            throw new IllegalArgumentException("Unknown version " + v);
        }
    }

    @Deprecated
    public static boolean isValid(byte v) {
        return v == V_10.version || v == V_11.version;
    }

}
