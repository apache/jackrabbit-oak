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
package org.apache.jackrabbit.oak.segment.standby.codec;

import org.apache.jackrabbit.oak.segment.Segment;

public class SegmentReply {

    public static final int SEGMENT = 0;
    public static final int BLOB = 1;

    public static SegmentReply empty() {
        return new SegmentReply();
    }

    private final int type;

    private final Segment segment;

    private final IdArrayBasedBlob blob;

    public SegmentReply(Segment segment) {
        this.type = SEGMENT;
        this.segment = segment;
        this.blob = null;
    }

    public SegmentReply(IdArrayBasedBlob blob) {
        this.type = BLOB;
        this.segment = null;
        this.blob = blob;
    }

    private SegmentReply() {
        this.type = -1;
        this.segment = null;
        this.blob = null;
    }

    public Segment getSegment() {
        return this.segment;
    }

    public IdArrayBasedBlob getBlob() {
        return blob;
    }

    public int getType() {
        return type;
    }

}
