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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinitionProvider;

/**
 * PrivilegeDefinitionProviderImpl... TODO
 *
 * TODO: review if jcr:all should be present in the content as well (updated in the privilege commit validator)
 */
class PrivilegeDefinitionProviderImpl implements PrivilegeDefinitionProvider, PrivilegeConstants {

    private final ContentSession contentSession;
    private final Root root;

    PrivilegeDefinitionProviderImpl(ContentSession contentSession, Root root) {
        this.contentSession = contentSession;
        this.root = root;
    }

    //--------------------------------------------------< PrivilegeProvider >---

    @Override
    public PrivilegeDefinition[] getPrivilegeDefinitions() {
        Map<String, PrivilegeDefinition> definitions = getReader().readDefinitions();
        definitions.put(JCR_ALL, getJcrAllDefinition(definitions));
        return definitions.values().toArray(new PrivilegeDefinition[definitions.size()]);
    }

    @Override
    public PrivilegeDefinition getPrivilegeDefinition(String name) {
        if (JCR_ALL.equals(name)) {
            return getJcrAllDefinition(getReader().readDefinitions());
        } else {
            return getReader().readDefinition(name);
        }
    }

    @Override
    public PrivilegeDefinition registerDefinition(
            final String privilegeName, final boolean isAbstract,
            final Set<String> declaredAggregateNames)
            throws RepositoryException {

        PrivilegeDefinition definition = new PrivilegeDefinitionImpl(privilegeName, isAbstract, declaredAggregateNames);
        PrivilegeDefinitionWriter writer = new PrivilegeDefinitionWriter(contentSession.getLatestRoot());
        writer.writeDefinition(definition);

        // refresh the current root to make sure the definition is visible
        root.refresh();

        return definition;
    }

    //------------------------------------------------------------< private >---

    private PrivilegeDefinitionReader getReader() {
        return new PrivilegeDefinitionReader(root);
    }

    @Nonnull
    private static PrivilegeDefinition getJcrAllDefinition(Map<String, PrivilegeDefinition> definitions) {
        return new PrivilegeDefinitionImpl(JCR_ALL, false, definitions.keySet());
    }
}