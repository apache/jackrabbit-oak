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
package org.apache.jackrabbit.oak.api;

import org.apache.jackrabbit.mk.model.NodeBuilder;
import org.apache.jackrabbit.mk.model.NodeState;

/**
 * The {@code Connection} interface ...
 *
 * TODO: define whether this is a repository-level connection or just bound to a single workspace.
 * TODO: describe how this interface is intended to handle validation: nt, names, ac, constraints...
 */
public interface Connection {

    NodeState getCurrentRoot();

    NodeState commit(NodeState newRoot) throws CommitFailedException;

    NodeBuilder getNodeBuilder(NodeState state);

    // TODO : add versioning operations

    // TODO : add query execution operations

}