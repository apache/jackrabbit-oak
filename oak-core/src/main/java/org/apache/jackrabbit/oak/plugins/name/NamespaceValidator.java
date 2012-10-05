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

import java.util.Locale;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;

import static org.apache.jackrabbit.oak.api.Type.STRING;

class NamespaceValidator extends DefaultValidator {

    private final Map<String, String> map;

    public NamespaceValidator(Map<String, String> map) {
        this.map = map;
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        String prefix = after.getName();
        // ignore jcr:primaryType
        if (prefix.equals("jcr:primaryType")) {
            return;
        }
        if (map.containsKey(prefix)) {
            throw new NamespaceValidatorException(
                    "Namespace mapping already registered", prefix);
        } else if (Namespaces.isValidPrefix(prefix)) {
            if (after.isArray() || !STRING.equals(after.getType())) {
                throw new NamespaceValidatorException(
                        "Invalid namespace mapping", prefix);
            } else if (prefix.toLowerCase(Locale.ENGLISH).startsWith("xml")) {
                throw new NamespaceValidatorException(
                        "XML prefixes are reserved", prefix);
            } else if (map.containsValue(after.getValue(STRING))) {
                throw modificationNotAllowed(prefix);
            }
        } else {
            throw new NamespaceValidatorException(
                    "Not a valid namespace prefix", prefix);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        if (map.containsKey(after.getName())) {
            throw modificationNotAllowed(after.getName());
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        if (map.containsKey(before.getName())) {
            // TODO: Check whether this namespace is still used in content
        }
    }

    private static NamespaceValidatorException modificationNotAllowed(String prefix) {
        return new NamespaceValidatorException(
                "Namespace modification not allowed", prefix);
    }

}
