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

import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.kernel.NodeState;
import org.apache.jackrabbit.oak.kernel.Validator;

class NameValidator implements Validator {

    private final Set<String> prefixes;

    public NameValidator(Set<String> prefixes) {
        this.prefixes = prefixes;
    }

    protected void checkValidName(String name) throws CommitFailedException {
        String prefix = null;
        String local = name;

        int colon = name.indexOf(':');
        if (colon != -1) {
            prefix = name.substring(0, colon);
            local = name.substring(colon + 1);
        }

        if (!(prefix == null || prefixes.contains(prefix))
                || !isValidLocalName(local)) {
            throw new CommitFailedException(
                    "Self or parent paths (. or ..) are not valid as names");
        }
    }

    private boolean isValidLocalName(String local) {
        if (".".equals(local) || "..".equals(local)) {
            return false;
        }

        for (int i = 0; i < local.length(); i++) {
            char ch = local.charAt(i);
            if ("/:[]|*".indexOf(ch) != -1) { // TODO: XMLChar check
                return false;
            }
        }

        return true;
    }

    //-------------------------------------------------------< NodeValidator >

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        checkValidName(after.getName());
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        // do nothing
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
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        return this;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        // do nothing
        return null;
    }

}
