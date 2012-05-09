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

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class NameValidatorProvider implements ValidatorProvider {

    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        Set<String> prefixes = new HashSet<String>();

        // Default JCR prefixes are always available
        prefixes.add("jcr");
        prefixes.add("nt");
        prefixes.add("mix");
        prefixes.add("sv");

        // Jackrabbit 2.x prefixes are always available
        prefixes.add("rep");

        // Find any extra prefixes from /jcr:system/jcr:namespaces
        NodeState system = after.getChildNode("jcr:system");
        if (system != null) {
            NodeState registry = system.getChildNode("jcr:namespaces");
            if (registry != null) {
                for (PropertyState property : registry.getProperties()) {
                    prefixes.add(property.getName());
                }
            }
        }

        return new NameValidator(prefixes);
    }

}
