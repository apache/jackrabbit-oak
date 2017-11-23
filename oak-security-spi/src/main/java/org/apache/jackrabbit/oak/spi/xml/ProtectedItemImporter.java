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
package org.apache.jackrabbit.oak.spi.xml;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

/**
 * Base interface for {@link ProtectedNodeImporter} and {@link ProtectedPropertyImporter}.
 */
public interface ProtectedItemImporter {

    /**
     * Parameter name for the import behavior configuration option.
     */
    String PARAM_IMPORT_BEHAVIOR = "importBehavior";

    /**
     * Initializes the importer.
     *
     * @param session The session that is running the import.
     * @param root The root associated with the import.
     * @param namePathMapper The name/path mapper used to translate names
     * between their jcr and oak form.
     * @param isWorkspaceImport A flag indicating whether the import has been
     * started from the {@link javax.jcr.Workspace} or from the
     * {@link javax.jcr.Session}. Implementations are free to implement both
     * types of imports or only a single one. For example it doesn't make sense
     * to allow for importing versions along with a Session import as
     * version operations are required to never leave transient changes behind.
     * @param uuidBehavior The uuid behavior specified with the import call.
     * @param referenceTracker The uuid/reference helper.
     * @param securityProvider The security provider.
     * @return {@code true} if this importer was successfully initialized and
     * is able to handle an import with the given setup; {@code false} otherwise.
     */
    boolean init(@Nonnull Session session, @Nonnull Root root,
            @Nonnull NamePathMapper namePathMapper,
            boolean isWorkspaceImport, int uuidBehavior,
            @Nonnull ReferenceChangeTracker referenceTracker,
            @Nonnull SecurityProvider securityProvider);

    /**
     * Post processing protected reference properties underneath a protected
     * or non-protected parent node. If the parent is protected it has been
     * handled by this importer already.
     *
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    void processReferences() throws RepositoryException;
}