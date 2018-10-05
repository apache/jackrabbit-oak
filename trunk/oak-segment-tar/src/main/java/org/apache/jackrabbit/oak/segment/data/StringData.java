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

package org.apache.jackrabbit.oak.segment.data;

public class StringData {

    private final String string;

    private final RecordIdData recordId;

    private final int length;

    StringData(String string, int length) {
        this.string = string;
        this.length = length;
        this.recordId = null;
    }

    StringData(RecordIdData recordId, int length) {
        this.recordId = recordId;
        this.length = length;
        this.string = null;
    }

    public boolean isString() {
        return string != null;
    }

    public boolean isRecordId() {
        return recordId != null;
    }

    public String getString() {
        return string;
    }

    public RecordIdData getRecordId() {
         return recordId;
    }

    public int getLength() {
        return length;
    }

}
