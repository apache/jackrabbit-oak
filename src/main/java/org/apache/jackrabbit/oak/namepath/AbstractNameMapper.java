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

public abstract class AbstractNameMapper implements NameMapper {

    protected abstract String getJcrPrefix(String oakPrefix);

    protected abstract String getOakPrefix(String jcrPrefix);

    protected abstract String getOakPrefixFromURI(String uri);

    @Override
    public String getOakName(String jcrName) {

        int pos = jcrName.indexOf(':');
        
        if (pos < 0) {
            // no colon
            return jcrName.startsWith("{}") ? jcrName.substring(2) : jcrName;
        } else {
            if (jcrName.charAt(0) == '{') {
                int endpos = jcrName.indexOf('}');
                if (endpos > pos) {
                    // expanded name

                    String nsuri = jcrName.substring(1, endpos);
                    String name = jcrName.substring(endpos + 1);

                    String oakPref = getOakPrefixFromURI(nsuri);
                    if (oakPref == null) {
                        // TODO
                        return null;
                    } else {
                        return oakPref + ':' + name;
                    }
                }
            }

            // otherwise: not an expanded name

            String pref = jcrName.substring(0, pos);
            String name = jcrName.substring(pos + 1);
            String oakPrefix = getOakPrefix(pref);
            if (oakPrefix == null) {
                return null; // not a mapped name
            } else {
                return oakPrefix + ':' + name;
            }
        }
    }

    @Override
    public String getJcrName(String oakName) {
        int pos = oakName.indexOf(':');
        if (pos < 0) {
            // non-prefixed
            return oakName;
        } else {
            String pref = oakName.substring(0, pos);
            String name = oakName.substring(pos + 1);

            if (pref.startsWith("{")) {
                throw new IllegalStateException(
                        "invalid oak name (maybe expanded name leaked out?): "
                                + oakName);
            }

            String jcrPrefix = getJcrPrefix(pref);
            if (jcrPrefix == null) {
                throw new IllegalStateException("invalid oak name: " + oakName);
            } else {
                return jcrPrefix + ':' + name;
            }
        }
    }

}
