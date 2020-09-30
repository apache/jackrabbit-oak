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
package org.apache.jackrabbit.api.security.principal;

import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;

/**
 * <code>ItemBasedPrincipal</code> is a <code>Principal</code> having a
 * corresponding item within the JCR repository. In addition to the methods
 * inherited from the {@link java.security.Principal} interface it therefore
 * provides a {@link #getPath()} method.
 */
public interface ItemBasedPrincipal extends JackrabbitPrincipal {

    /**
     * Returns the JCR path of the item that corresponds to this
     * <code>Principal</code>.
     * 
     * @return the path of the {@link javax.jcr.Item} that corresponds to this
     * <code>Principal</code>.
     * @throws RepositoryException If an error occurs while retrieving the
     * <code>Item</code> path.
     */
    @NotNull
    String getPath() throws RepositoryException;
}
