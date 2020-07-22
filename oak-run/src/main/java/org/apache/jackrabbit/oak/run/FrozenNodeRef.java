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
package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.oak.commons.PathUtils;

import com.google.common.collect.Iterables;

/**
 * Contains one particular reference to an nt:frozenNode.
 */
public class FrozenNodeRef {

    static final String REFERENCE_TO_NT_FROZEN_NODE_FOUND_PREFIX = "Reference to nt:frozenNode found : ";

    private final String refPath;
    private final String refName;
    private final String refType;
    private final String refValue;
    private final String targetPath;

    static boolean isFrozenNodeReferenceCandidate(String uuidRefPath) {
        boolean isVersionStorage = PathUtils.isAncestor("/jcr:system/jcr:versionStorage", uuidRefPath);
        if (!isVersionStorage) {
            // TODO: we could consider NOT continuing here, as an nt:frozenNode could "in theory"
            // also show up somewhere else - but it is slightly faster this way..
            return false;
        }
        int depth = PathUtils.getDepth(uuidRefPath);
        if (depth <= 7) {
            // TODO: we could consider NOT continuing here, as the structure of /jcr:system/jcr:versionStorage
            // could at some point change and this '7' no longer be correct - but so far it is and it's faster this way
            return false;
        }
        boolean containsJcrFrozenNodeName = Iterables.contains(PathUtils.elements(uuidRefPath), "jcr:frozenNode");
        if (!containsJcrFrozenNodeName) {
            // TODO: we could consider NOT continuing here, as we might opt to store frozenNodes
            // under another parent than 'jcr:frozenNode' - but so far that's how it is and it's faster
            return false;
        }
        return true;
    }

    FrozenNodeRef(String refPropertyPath, String refPropertyName, String refPropertyType, String refPropertyValue, String targetPath) {
        this.refPath = refPropertyPath;
        this.refName = refPropertyName;
        this.refType = refPropertyType;
        this.refValue = refPropertyValue;
        this.targetPath = targetPath;
    }

    String toInfoString() {
        return "Node " + refPath + " has a property '" + refName + "'" + " (of type " + refType + ")" + " with a value '"
                + refValue + "'" + " which points to an nt:frozenNode, namely to: " + targetPath;
    }
}
