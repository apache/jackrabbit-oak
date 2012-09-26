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
package org.apache.jackrabbit.oak.namepath;

import javax.annotation.CheckForNull;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.name.ReadOnlyNamespaceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameMapperImpl extends AbstractNameMapper {
    private static final Logger log = LoggerFactory.getLogger(NameMapperImpl.class);

    private final Tree root;
    private final NamespaceRegistry nsReg = new ReadOnlyNamespaceRegistry() {
        @Override
        protected Tree getReadTree() {
            return root;
        }
    };

    public NameMapperImpl(Tree root) {
        this.root = root;
    }

    @Override
    @CheckForNull
    protected String getJcrPrefix(String oakPrefix) {
        return oakPrefix;
    }

    @Override
    @CheckForNull
    protected String getOakPrefix(String jcrPrefix) {
        return jcrPrefix;
    }

    @Override
    @CheckForNull
    protected String getOakPrefixFromURI(String uri) {
        try {
            return nsReg.getPrefix(uri);
        } catch (RepositoryException e) {
            log.debug("Could not get OAK prefix for URI " + uri);
            return null;
        }
    }

    @Override
    public boolean hasSessionLocalMappings() {
        return false;
    }
}
