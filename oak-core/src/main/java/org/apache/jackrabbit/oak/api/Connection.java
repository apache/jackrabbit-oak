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

import java.io.Closeable;

/**
 * The {@code Connection} interface ...
 *
 * - retrieving information from persistent layer (MK) that are accessible to
 *   a given session
 *
 * - validate information being written back to the persistent layer. this includes
 *   permission evaluation, node type and name constraints etc.
 *
 * - update the revision ID a given session is operating on when certain actions
 *   take place (save, refresh, TBD)
 *
 * - Provide access to authentication and authorization related information
 *
 * - Connection instances are not thread safe.
 *
 * - The implementation of this and all related interfaces are intended to only
 *   hold the state of the persistent layer at a given revision without any
 *   session-related state modifications.
 *
 * - Whether this connection is bound to a workspace or to the whole repository
 *   is determined by how the connection was acquired: connections acquired by
 *   {@link RepositoryService#login(Object, String)} are bound to the workspace
 *   passed to the login method. Connections acquired by
 *   {@link Connection#getRepositoryConnection()} are bound to the whole repository.
 *
 * TODO: describe how this interface is intended to handle validation: nt, names, ac, constraints...
 */
public interface Connection extends Closeable {

    /**
     * This methods provides access to information related to authentication
     * and authorization of this connection. Multiple calls to this method
     * may return different instances which are guaranteed to be equal wrt.
     * to {@link Object#equals(Object)}.
     *
     * @return  immutable {@link AuthInfo} instance
     */
    AuthInfo getAuthInfo();

    /**
     * The immutable name of the workspace this {@code Connection} instance has
     * been created for. If no workspace name has been specified during
     * repository login this method will return the name of the default
     * workspace.
     *
     * @return name of the workspace this instance has been created for or
     * {@code null} if this connection is repository bound.
     */
    String getWorkspaceName();

    /**
     * Provide access to the whole repository (i.e. all workspaces) respecting
     * the access rights of the current connection. This method returns the
     * same connection instance on every invocation.
     *
     * @return a {@code Connection} which covers the whole repository.
     */
    Connection getRepositoryConnection();

    NodeState getCurrentRoot();

    NodeState commit(NodeState newRoot) throws CommitFailedException;

    NodeBuilder getNodeBuilder(NodeState state);

    // TODO : add versioning operations

    // TODO : add query execution operations

}