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
package org.apache.jackrabbit.oak.namepath;

import java.util.HashMap;
import java.util.Map;

/**
 * Prove of concept implementation for OAK-61.
 *
 * This implementation is entirely in memory. TODO: persist mappings
 *
 * For each registered mapping from a jcr prefix to a namespace a
 * a mk prefix is generated. The mk prefixes are in one to one relation
 * ship with the registered namespaces and should be used as shorthands
 * in place of the actual namespaces in all further name and path handling.
 *
 * TODO: expose the relevant methods through the Oak API.
 */
public class NamespaceMappings {
    private final Map<String, String> jcr2NsMap = new HashMap<String, String>();
    private final Map<String, String> ns2MkMap = new HashMap<String, String>();
    private final Map<String, String> mk2JcrMap = new HashMap<String, String>();

    /**
     * Add a mapping jcr prefix to namespace mapping. If either
     * {@code jcrPrefix} or {@code namespace} is already mapped, the
     * existing mapping is removed first.
     *
     * @param jcrPrefix
     * @param namespace
     */
    public void registerNamespace(String jcrPrefix, String namespace) {
        if (jcrPrefix == null || namespace == null) {
            throw new IllegalArgumentException();
        }

        unregisterJcrPrefix(jcrPrefix);
        unregisterNamespace(namespace);

        String mk = ns2MkMap.get(namespace);
        if (mk == null) {
            // Generate a mk prefix. Use jcrPrefix if possible, disambiguate otherwise
            mk = jcrPrefix;
            for (int i = 2; ns2MkMap.containsValue(mk); i++) {
                mk = jcrPrefix + i;
            }
            ns2MkMap.put(namespace, mk);
        }

        mk2JcrMap.put(mk, jcrPrefix);
        jcr2NsMap.put(jcrPrefix, namespace);
    }

    /**
     * Remove the mapping of {@code jcrPrefix} if such exists.
     * @param jcrPrefix
     * @return  the namespace to which {@code jcrPrefix} mapped or
     * {@code null} if none.
     */
    public String unregisterJcrPrefix(String jcrPrefix) {
        if (jcrPrefix == null) {
            throw new IllegalArgumentException();
        }

        String ns = jcr2NsMap.remove(jcrPrefix);
        mk2JcrMap.remove(ns2MkMap.get(ns));
        return ns;
    }

    /**
     * Remove the mapping for {@code namespace} if such exists.
     * @param namespace
     * @return  the jcr prefix which mapped to {@code namespace} or
     * {@code null} if none.
     */
    public String unregisterNamespace(String namespace) {
        if (namespace == null) {
            throw new IllegalArgumentException();
        }

        String jcrPrefix = mk2JcrMap.remove(ns2MkMap.get(namespace));
        jcr2NsMap.remove(jcrPrefix);
        return jcrPrefix;
    }

    /**
     * Retrieve the namespace which {@code jcrPrefix} maps to if any
     * or {@code null} otherwise.
     * @param jcrPrefix
     * @return  namespace or {@code null}
     */
    public String getNamespace(String jcrPrefix) {
        return jcr2NsMap.get(jcrPrefix);
    }

    /**
     * Retrieve the jcr prefix which maps to {@code namespace} if any
     * or {@code null} otherwise.
     * @param namespace
     * @return  jcr prefix or {@code null}
     */
    public String getJcrPrefix(String namespace) {
        return mk2JcrMap.get(ns2MkMap.get(namespace));
    }

    /**
     * Return the registered namespaces
     * @return
     */
    public String[] getNamespaces() {
        return jcr2NsMap.values().toArray(new String[jcr2NsMap.size()]);
    }

    /**
     * Return the registered jcr prefixes
     * @return
     */
    public String[] getJcrPrefixes() {
        return jcr2NsMap.keySet().toArray(new String[jcr2NsMap.size()]);
    }

    //------------------------------------------------------------< internal >---

    /**
     * Retrieve the jcr prefix which maps to {@code mkPrefix} if any
     * or {@code null} otherwise.
     * @param mkPrefix
     * @return  jcr prefix or {@code null}
     */
    String getJcrPrefixFromMk(String mkPrefix) {
        return mk2JcrMap.get(mkPrefix);
    }

    /**
     * Retrieve the mk prefix which maps to {@code jcrPrefix} if any
     * or {@code null} otherwise.
     * @param jcrPrefix
     * @return  mk prefix or {@code null}
     */
    String getMkPrefixFromJcr(String jcrPrefix) {
        return ns2MkMap.get(jcr2NsMap.get(jcrPrefix));
    }

}
