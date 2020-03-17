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

import java.util.Comparator;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

/**
 * Implements a comparator, which sorts NodeDocumentId string according to 1) their
 * depth (highest first) and 2) the id string itself.
 */
public class NodeDocumentIdComparator implements Comparator<String> {
    public static final Comparator<String> INSTANCE = new NodeDocumentIdComparator();

    private NodeDocumentIdComparator() {
    }

    @Override
    public int compare(String o1, String o2) {
        int d1 = Utils.getDepthFromId(o1);
        int d2 = Utils.getDepthFromId(o2);
        if (d1 != d2) {
            return Integer.signum(d2 - d1);
        }
        return o1.compareTo(o2);
    }
}
