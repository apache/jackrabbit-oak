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
package org.apache.jackrabbit.oak.jcr;

import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import java.util.HashMap;
import java.util.Map;

/**
 * A naive implementation of {@link NamespaceRegistry}, hard-wiring the
 * predefined namespaces for now.
 */
public class NamespaceRegistryImpl implements NamespaceRegistry {
    private final Map<String, String> map;

    public NamespaceRegistryImpl() {
        map = new HashMap<String, String>();
        map.put(PREFIX_EMPTY, NAMESPACE_EMPTY);
        map.put(PREFIX_JCR, NAMESPACE_JCR);
        map.put(PREFIX_MIX, NAMESPACE_MIX);
        map.put(PREFIX_NT, NAMESPACE_NT);
        map.put(PREFIX_XML, NAMESPACE_XML);
        map.put("sv", "http://www.jcp.org/jcr/sv/1.0");
    }

    @Override
    public void registerNamespace(String prefix, String uri) throws RepositoryException {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterNamespace(String prefix) throws RepositoryException {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getPrefixes() throws RepositoryException {
        return map.keySet().toArray(new String[map.size()]);
    }

    @Override
    public String[] getURIs() throws RepositoryException {
        return map.values().toArray(new String[map.size()]);
    }

    @Override
    public String getURI(String prefix) throws RepositoryException {
        String result = map.get(prefix);
        if (result == null) {
            throw new NamespaceException();
        }
        return result;
    }

    @Override
    public String getPrefix(String uri) throws RepositoryException {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue().equals(uri)) {
                return entry.getKey();
            }
        }
        throw new NamespaceException();
    }
}
