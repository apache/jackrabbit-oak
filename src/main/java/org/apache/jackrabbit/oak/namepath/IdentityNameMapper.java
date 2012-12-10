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

import java.util.Map;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;

/**
 * Name mapper with no local prefix remappings. URI to prefix mappings
 * are read from the repository when needed.
 */
public class IdentityNameMapper implements NameMapper {

    private final Tree root;

    public IdentityNameMapper(Tree root) {
        this.root = root;
    }

    @Override @CheckForNull
    public String getJcrName(String oakName) {
        assert !oakName.startsWith(":"); // hidden name
        return oakName;
    }

    @Override @CheckForNull
    public String getOakName(String jcrName) {
        if (jcrName.startsWith("{")) { // Could it be an expanded name?
            int brace = jcrName.indexOf('}', 1);
            String uri = jcrName.substring(1, brace);
            if (uri.isEmpty()) {
                return jcrName.substring(2); // special case: {}name
            } else if (uri.indexOf(':') != -1) {
                // It's an expanded name, look up the namespace prefix
                String name = jcrName.substring(brace + 1);
                for (Map.Entry<String, String> entry
                        : Namespaces.getNamespaceMap(root).entrySet()) {
                    if (uri.equals(entry.getValue())) {
                        return entry.getKey() + ':' + name;
                    }
                }
                return null;
            }
        }

        return jcrName;
    }

    @Override
    public boolean hasSessionLocalMappings() {
        return false;
    }

}
