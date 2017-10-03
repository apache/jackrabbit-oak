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

package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.io.IOException;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;

public class PathStoredFieldVisitor extends StoredFieldVisitor {

    private String path;
    private boolean pathVisited;

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (PATH.equals(fieldInfo.name)) {
            return Status.YES;
        }
        return pathVisited ? Status.STOP : Status.NO;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value)
            throws IOException {
        if (PATH.equals(fieldInfo.name)) {
            path = value;
            pathVisited = true;
        }
    }

    public String getPath() {
        return path;
    }
}
