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
package org.apache.jackrabbit.oak.spi.lifecycle;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Initializer of repository content. A component that needs to add specific
 * content to a new repository can implement this interface. Then when a
 * repository becomes available, all the configured initializers are invoked
 * in sequence.
 */
public interface RepositoryInitializer {

    /**
     * Default implementation makes no changes to the repository.
     */
    RepositoryInitializer DEFAULT = new RepositoryInitializer() {
        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
        }
    };

    /**
     * Initializes repository content. This method is called as soon as a
     * repository becomes available. Note that the repository may already
     * have been initialized, so the implementation of this method should
     * check for that before blindly adding new content.
     *
     * @param builder builder for accessing and modifying repository content
     */
    void initialize(@Nonnull NodeBuilder builder);
}
