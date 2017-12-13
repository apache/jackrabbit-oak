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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.google.common.collect.ImmutableList;

/**
 * Implementation of the {@code RepositoryInitializer} interface responsible for
 * setting up query indices for the system maintained, protected properties defined
 * by this module:
 *
 * <ul>
 *     <li>Index Definition <i>externalPrincipalNames</i>: Indexing
 *     {@link ExternalIdentityConstants#REP_EXTERNAL_PRINCIPAL_NAMES} properties.
 *     This index is used by the {@link ExternalGroupPrincipalProvider} to lookup
 *     and find principals stored in this property.</li>
 * </ul>
 *
 * @since Oak 1.5.3
 */
class ExternalIdentityRepositoryInitializer implements RepositoryInitializer {

    private final boolean enforceUniqueIds;

    ExternalIdentityRepositoryInitializer(boolean enforceUniqueIds) {
        this.enforceUniqueIds = enforceUniqueIds;
    }

    @Override
    public void initialize(@Nonnull NodeBuilder builder) {

        // create index definition for "rep:externalId" and
        // "rep:externalPrincipalNames"
        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);
        if (enforceUniqueIds && !index.hasChildNode("externalId")) {
            NodeBuilder definition = IndexUtils.createIndexDefinition(index, "externalId", true, true,
                    ImmutableList.of(ExternalIdentityConstants.REP_EXTERNAL_ID), null);
            definition.setProperty("info", "Oak index assuring uniqueness of rep:externalId properties.");
        }
        if (!index.hasChildNode("externalPrincipalNames")) {
            NodeBuilder definition = IndexUtils.createIndexDefinition(index, "externalPrincipalNames", true, false,
                    ImmutableList.of(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES), null);
            definition.setProperty("info",
                    "Oak index used by the principal management provided by the external authentication module.");
        }
    }
}