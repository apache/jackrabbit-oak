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
package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

/**
 * {@code LdapIdentityProperties} implements a case insensitive hash map that preserves the case of the keys but
 * ignores the case during lookup.
 */
public class LdapIdentityProperties extends HashMap<String, Object> {

    private final Map<String, String> keyMapping = new HashMap<String, String>();

    public LdapIdentityProperties(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public LdapIdentityProperties(int initialCapacity) {
        super(initialCapacity);
    }

    public LdapIdentityProperties() {
        super();
    }

    public LdapIdentityProperties(Map<? extends String, ?> m) {
        super(m);
        for (String key: m.keySet()) {
            keyMapping.put(convert(key), key);
        }
    }

    @Override
    public Object put(String key, Object value) {
        keyMapping.put(convert(key), key);
        return super.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        keyMapping.remove(convert(key));
        return super.remove(key);
    }

    @Override
    public Object get(Object key) {
        String realKey = keyMapping.get(convert(key));
        return realKey == null ? null : super.get(realKey);
    }

    @Override
    public boolean containsKey(Object key) {
        String realKey = keyMapping.get(convert(key));
        return realKey != null && super.containsKey(realKey);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        super.putAll(m);
        for (String key: m.keySet()) {
            keyMapping.put(convert(key), key);
        }
    }

    @Override
    public void clear() {
        super.clear();
        keyMapping.clear();
    }

    @CheckForNull
    private String convert(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        String key = obj instanceof String ? (String) obj : String.valueOf(obj);
        return key.toUpperCase(Locale.ENGLISH).toLowerCase(Locale.ENGLISH);
    }
}