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
package org.apache.jackrabbit.api.jmx;

import java.util.Map;

import javax.jcr.RepositoryException;

/**
 * Interface for managing a JCR repository as a JMX MBean.
 *
 * @since Apache Jackrabbit 2.3
 */
public interface ManagedRepositoryMBean {

    /**
     * Returns the name of this repository implementation.
     *
     * @see javax.jcr.Repository#REP_NAME_DESC
     * @return name of this repository implementation
     */
    String getName();

    /**
     * Returns the version of this repository implementation.
     *
     * @see javax.jcr.Repository#REP_VERSION_DESC
     * @return version of this repository implementation
     */
    String getVersion();

    /**
     * Returns all the repository descriptors.
     *
     * @return repository descriptors
     */
    Map<String, String> getDescriptors();

    /**
     * Returns the names of all the workspaces in this repository.
     *
     * @return workspace names
     */
    String[] getWorkspaceNames();

    /**
     * Creates a new workspace with the given name.
     *
     * @param name workspace name
     * @throws RepositoryException if the workspace could not be created
     */
    void createWorkspace(String name) throws RepositoryException;

}
