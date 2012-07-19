/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.jackrabbit.oak.plugins.name;

import java.util.HashMap;
import java.util.Map;

import javax.jcr.NamespaceRegistry;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * Internal static utility class for managing the persisted namespace registry.
 */
class Namespaces {

    private static final Map<String, String> defaults = new HashMap<String, String>();

    // TODO: this should not use the "jcr" prefix
    public static final String NSMAPNODENAME = "jcr:namespaces";

    private Namespaces() {
    }

    static {
        // Standard namespace specified by JCR (default one not included)
        defaults.put(NamespaceRegistry.PREFIX_EMPTY, NamespaceRegistry.NAMESPACE_EMPTY);
        defaults.put(NamespaceRegistry.PREFIX_JCR, NamespaceRegistry.NAMESPACE_JCR);
        defaults.put(NamespaceRegistry.PREFIX_NT,  NamespaceRegistry.NAMESPACE_NT);
        defaults.put(NamespaceRegistry.PREFIX_MIX, NamespaceRegistry.NAMESPACE_MIX);
        defaults.put(NamespaceRegistry.PREFIX_XML, NamespaceRegistry.NAMESPACE_XML);

        // Namespace included in Jackrabbit 2.x
        // TODO: use constants (see also http://java.net/jira/browse/JSR_333-50)
        defaults.put("sv", "http://www.jcp.org/jcr/sv/1.0");

        // TODO: see OAK-74
        defaults.put("rep", "internal");
    }

    public static Map<String, String> getNamespaceMap(Tree root) {
        Map<String, String> map = new HashMap<String, String>(defaults);

        Tree system = root.getChild(JcrConstants.JCR_SYSTEM);
        if (system != null) {
            Tree namespaces = system.getChild(NSMAPNODENAME);
            if (namespaces != null) {
                for (PropertyState property : namespaces.getProperties()) {
                    String prefix = property.getName();
                    if (!property.isArray() && isValidPrefix(prefix)) {
                        CoreValue value = property.getValue();
                        if (value.getType() == PropertyType.STRING) {
                            map.put(prefix, value.getString());
                        }
                    }
                }
            }
        }

        return map;
    }

    public static boolean isValidPrefix(String prefix) {
        // TODO: Other prefix rules?
        return !prefix.isEmpty() && prefix.indexOf(':') == -1;
    }

    public static boolean isValidLocalName(String local) {
        if (local.isEmpty() || ".".equals(local) || "..".equals(local)) {
            return false;
        }

        for (int i = 0; i < local.length(); i++) {
            char ch = local.charAt(i);
            if ("/:[]|*".indexOf(ch) != -1) { // TODO: XMLChar check
                return false;
            }
        }

        // TODO: Other name rules?
        return true;
    }

}
