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

package org.apache.jackrabbit.oak.spi.state;

/**
 * Enum to define various types of conflicts.
 *
 * @see org.apache.jackrabbit.oak.spi.state.NodeStore#rebase(NodeBuilder)
 */
public enum ConflictType {

    /**
     * A property has been removed while a property of the same name has been changed in trunk.
     */
    DELETE_CHANGED_PROPERTY("deleteChangedProperty",false),

    /**
     * A node has been removed while a node of the same name has been changed in trunk.
     */
    DELETE_CHANGED_NODE("deleteChangedNode",true),

    /**
     * A property has been added that has a different value than a property with the same name
     * that has been added in trunk.
     */
    ADD_EXISTING_PROPERTY("addExistingProperty",false),

    /**
     * A property has been changed while a property of the same name has been removed in trunk.
     */
    CHANGE_DELETED_PROPERTY("changeDeletedProperty",false),

    /**
     * A property has been changed while a property of the same name has been changed to a
     * different value in trunk.
     */
    CHANGE_CHANGED_PROPERTY("changeChangedProperty",false),

    /**
     * A property has been removed while a property of the same name has been removed in trunk.
     */
    DELETE_DELETED_PROPERTY("deleteDeletedProperty",false),

    /**
     * A node has been added that is different from a node of them same name that has been added
     * to the trunk.
     */
    ADD_EXISTING_NODE("addExistingNode",true),

    /**
     * A node has been changed while a node of the same name has been removed in trunk.
     */
    CHANGE_DELETED_NODE("changeDeletedNode",true),

    /**
     * A node has been removed while a node of the same name has been removed in trunk.
     */
    DELETE_DELETED_NODE("deleteDeletedNode",true),
    ;

    public String getName() {
        return name;
    }

    public boolean effectsNode() {
        return effectsNode;
    }

    public static ConflictType fromName(String name) {
        for (ConflictType t : values()) {
            if (t.getName().equals(name)) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unrecognized conflictType: " + name);
    }

    private final String name;
    private final boolean effectsNode;

    private ConflictType(String value, boolean node) {
        this.name = value;
        this.effectsNode = node;
    }
}
