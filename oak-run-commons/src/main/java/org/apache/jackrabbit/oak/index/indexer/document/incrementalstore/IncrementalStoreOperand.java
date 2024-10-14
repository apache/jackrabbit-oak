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
package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

/**
 * The operation to perform. Operations are generated by either running a 'diff'
 * , or by 'top up' which is downloading the changes from MongoDB.
 *
 * When using the 'diff' command, we expect that the node doesn't exist yet for
 * the 'add' case, and that the node exists for the 'delete' and 'modify' case
 * (due to the nature of the 'diff'). The operations are then ADD, DELETE,
 * MODIFY.
 *
 * When using the 'top up', we don't know whether the node existed before or
 * not; we only get the updated state. That's why we use different operations:
 * REMOVE_IF_EXISTS and INSERT_OR_UPDATE.
 */
public enum IncrementalStoreOperand {

    // add a new node;
    // log a warning if it doesn't exist
    // (this is used by the 'diff' command)
    ADD("A"),

    // delete an existing node;
    // log a warning if it doesn't exist
    // (this is used by the 'diff' command)
    DELETE("D"),

    // modify an existing node;
    // log a warning if it doesn't exist
    // (this is used by the 'diff' command)
    MODIFY("M"),

    // remove a node that may or may not exist
    // (this operation may be used by the 'top up' command)
    REMOVE_IF_EXISTS("R"),

    // add or update a new or existing node
    // (this operation may be used by the 'top up' command)
    INSERT_OR_UPDATE("U");

    private final String operand;

    IncrementalStoreOperand(String operand) {
        this.operand = operand;
    }

    @Override
    public String toString() {
        return operand;
    }
}