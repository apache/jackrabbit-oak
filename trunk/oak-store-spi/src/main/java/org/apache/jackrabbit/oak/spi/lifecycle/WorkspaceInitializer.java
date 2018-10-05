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
package org.apache.jackrabbit.oak.spi.lifecycle;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Initializer of a workspace and it's initial content. A module that needs
 * to add content to a workspace can implement this interface.
 * <p>
 * TODO: define if/how runtime configuration changes may affect the workspace content.
 * TODO: review params of initialize()
 */
public interface WorkspaceInitializer {

    WorkspaceInitializer DEFAULT = new WorkspaceInitializer() {
        @Override
        public void initialize(NodeBuilder builder, String workspaceName) {
        }
    };

    /**
     * Initialize the content of a new workspace. This method is called before
     * the workspace becomes available.
     *
     * @param builder       builder for accessing and modifying the workspace
     * @param workspaceName The name of the workspace that is being initialized.
     */
    void initialize(NodeBuilder builder, String workspaceName);
}
