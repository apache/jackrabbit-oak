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
package org.apache.jackrabbit.mongomk.util;

import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeState;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * MongoMK specific utility class.
 */
public class MongoUtil {

    public static String fromMongoRepresentation(Long revisionId) {
        return String.valueOf(revisionId);
    }

    public static Long toMongoRepresentation(String revisionId) throws Exception {
        if (revisionId == null) {
            return null;
        }
        try {
            return Long.parseLong(revisionId);
        } catch (NumberFormatException e) {
            throw new Exception("Invalid revision id: " + revisionId);
        }
    }

    public static long getBaseRevision(String branchId) {
        int i = branchId.indexOf('-');
        checkArgument(i >= 0, "Not a branch id: " + branchId);
        return Long.parseLong(branchId.substring(0, i));
    }

    public static NodeState wrap(Node node) {
        return node != null? new MongoNodeState(node) : null;
    }

    public static String adjustPath(String path) {
        return (path == null || path.isEmpty()) ? "/" : path;
    }

    public static boolean isFiltered(String path) {
        return !"/".equals(path);
    }

    public static String fromMongoPropertyKey(String key) {
        if (key.startsWith("[dollar]")) {
            return key.replaceFirst("\\[dollar\\]", "\\$");
        }
        if (key.contains("[dot]")) {
            return key.replaceAll("\\[dot\\]", "\\.");
        }
        return key;
    }

    public static String toMongoPropertyKey(String key) {
        if (key.startsWith("$")) {
            return key.replaceFirst("\\$", "[dollar]");
        }
        if (key.contains(".")) {
            return key.replaceAll("\\.", "[dot]");
        }
        return key;
    }
}