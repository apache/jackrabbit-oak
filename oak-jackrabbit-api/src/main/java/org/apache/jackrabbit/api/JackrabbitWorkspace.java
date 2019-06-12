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
package org.apache.jackrabbit.api;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.xml.sax.InputSource;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.Workspace;

/**
 * The Jackrabbit workspace interface. This interface contains the
 * Jackrabbit-specific extensions to the JCR {@link Workspace} interface.
 */
public interface JackrabbitWorkspace extends Workspace {

    /**
     * Creates a workspace with the given name.
     *
     * @param workspaceName name of the new workspace
     * @throws AccessDeniedException if the current session is not allowed to
     *                               create the workspace
     * @throws RepositoryException   if a workspace with the given name
     *                               already exists or if another error occurs
     * @see #getAccessibleWorkspaceNames()
     */
    void createWorkspace(String workspaceName)
            throws AccessDeniedException, RepositoryException;

    /**
     * Creates a workspace with the given name and a workspace configuration
     * template.
     *
     * @param workspaceName name of the new workspace
     * @param workspaceTemplate the configuration template of the new workspace
     * @throws AccessDeniedException if the current session is not allowed to
     *                               create the workspace
     * @throws RepositoryException   if a workspace with the given name
     *                               already exists or if another error occurs
     * @see #getAccessibleWorkspaceNames()
     */
    void createWorkspace(String workspaceName, InputSource workspaceTemplate)
            throws AccessDeniedException, RepositoryException;

    /**
     * Returns the privilege manager.
     *
     * @return the privilege manager.
     * @throws RepositoryException If an error occurs.
     */
    PrivilegeManager getPrivilegeManager() throws RepositoryException;
}
