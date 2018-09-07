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

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NSDATA;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_PREFIXES;

import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
class NameValidator extends DefaultValidator {

    private static final Logger LOG = LoggerFactory.getLogger(NameValidator.class);

    private final NodeState namespaces;
    private final Set<String> prefixes;

    /**
     * Flag controlling the strictness of the namespace checks. if {@code true}
     * namespaces existence will not be checked, otherwise referencing a
     * non-existent namespace will cause a {@link CommitFailedException}.
     * 
     * Used only for the case where lucene index definitions are registered via
     * {@link RepositoryInitializer}s.
     */
    private final boolean initPhase;

    private final boolean strictInitialNSChecks = Boolean.getBoolean("oak.strictInitialNSChecks");

    NameValidator(NodeState namespaces, boolean initPhase) {
        this.namespaces = namespaces;
        this.prefixes = newHashSet(namespaces.getChildNode(REP_NSDATA).getStrings(REP_PREFIXES));
        this.initPhase = initPhase;
    }

    // escape non-printable non-USASCII characters using standard Java escapes
    protected static String getPrintableName(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= ' ' && c < 127) {
                sb.append(c);
            } else if (c == '\b') {
                sb.append("\\b");
            } else if (c == '\f') {
                sb.append("\\f");
            } else if (c == '\n') {
                sb.append("\\n");
            } else if (c == '\r') {
                sb.append("\\r");
            } else if (c == '\t') {
                sb.append("\\t");
            } else {
                sb.append(String.format("\\u%04x", (int) c));
            }
        }
        return sb.toString();
    }

    protected void checkValidName(String name) throws CommitFailedException {
        int colon = name.indexOf(':');
        if (colon > 0) {
            String prefix = name.substring(0, colon);
            checkPrefix(prefix);
        }

        String local = name.substring(colon + 1);

        int n = local.length();
        if (n > 3 && local.charAt(n - 1) == ']') {
            int i = n - 2;
            while (i > 1 && Character.isDigit(local.charAt(i))) {
                i--;
            }
            if (local.charAt(i) != '[') {
                throw new CommitFailedException(
                        CommitFailedException.NAME, 2, "Invalid name index in: " + getPrintableName(name));
            } else {
                local = local.substring(0, i);
            }
        }

        if (!Namespaces.isValidLocalName(local)) {
            throw new CommitFailedException(
                    CommitFailedException.NAME, 3, "Invalid name: " + getPrintableName(name));
        }
    }

    private void checkPrefix(String prefix) throws CommitFailedException {
        if (prefix.isEmpty() || !contains(prefixes, namespaces, prefix)) {
            String msg = "Invalid namespace prefix(" + prefixes + "): " + prefix;
            if (initPhase && !strictInitialNSChecks) {
                LOG.warn(msg);
                return;
            }
            throw new CommitFailedException(CommitFailedException.NAME, 1, msg);
        }
    }

    private static boolean contains(Set<String> prefixes, NodeState namespaces, String prefix) {
        return prefixes.contains(prefix) || Namespaces.collectNamespaces(namespaces.getProperties()).containsKey(prefix);
    }

    protected void checkValidValue(PropertyState property)
            throws CommitFailedException {
        if (Type.NAME.equals(property.getType()) || Type.NAMES.equals(property.getType())) {
            for (String value : property.getValue(Type.NAMES)) {
                checkValidValue(value);
            }
        } else if (Type.PATH.equals(property.getType()) || Type.PATHS.equals(property.getType())) {
            for (String value : property.getValue(Type.PATHS)) {
                for (String name: PathUtils.elements(value)) {
                    checkValidValue(name);
                }
            }
        }
    }

    protected void checkValidValue(String value)
            throws CommitFailedException {
        checkValidName(value);
    }

    //-------------------------------------------------------< NodeValidator >

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        checkValidName(after.getName());
        checkValidValue(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        checkValidValue(after);
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        // do nothing
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        checkValidName(name);
        return this;
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after) {
        return this;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        return null;
    }

}
