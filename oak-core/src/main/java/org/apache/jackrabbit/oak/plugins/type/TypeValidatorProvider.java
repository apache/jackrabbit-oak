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
package org.apache.jackrabbit.oak.plugins.type;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.commit.Validator;
import org.apache.jackrabbit.oak.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.kernel.ChildNodeEntry;
import org.apache.jackrabbit.oak.kernel.NodeState;

public class TypeValidatorProvider implements ValidatorProvider {

    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        Set<String> types = new HashSet<String>();

        // Default JCR types are always available
        types.add("nt:base");
        types.add("nt:hierarchyNode");
        types.add("nt:file");
        types.add("nt:linkedFile");
        types.add("nt:folder");
        types.add("nt:resource");
        types.add("mix:title");
        types.add("mix:created");
        types.add("mix:lastModified");
        types.add("mix:language");
        types.add("mix:mimeType");
        types.add("nt:address");
        types.add("mix:etag");
        types.add("nt:unstructured");
        types.add("mix:referenceable");
        types.add("mix:lockable");
        types.add("mix:shareable");
        types.add("mix:simpleVersionable");
        types.add("mix:versionable");
        types.add("nt:versionHistory");
        types.add("nt:versionLabels");
        types.add("nt:version");
        types.add("nt:frozenNode");
        types.add("nt:versionedChild");
        types.add("nt:activity");
        types.add("nt:configuration");
        types.add("nt:nodeType");
        types.add("nt:propertyDefinition");
        types.add("nt:childNodeDefinition");
        types.add("nt:query");
        types.add("mix:lifecycle");

        // Jackrabbit 2.x types are always available
        types.add("rep:root");
        types.add("rep:system");
        types.add("rep:nodeTypes");
        types.add("rep:versionStorage");
        types.add("rep:Activities");
        types.add("rep:Configurations");
        types.add("rep:VersionReference");
        types.add("rep:AccessControllable");
        types.add("rep:RepoAccessControllable");
        types.add("rep:Policy");
        types.add("rep:ACL");
        types.add("rep:ACE");
        types.add("rep:GrantACE");
        types.add("rep:DenyACE");
        types.add("rep:AccessControl");
        types.add("rep:PrincipalAccessControl");
        types.add("rep:Authorizable");
        types.add("rep:Impersonatable");
        types.add("rep:User");
        types.add("rep:Group");
        types.add("rep:AuthorizableFolder");
        types.add("rep:Members");
        types.add("rep:RetentionManageable");

        // Find any extra types from /jcr:system/jcr:nodeTypes
        NodeState system = after.getChildNode("jcr:system");
        if (system != null) {
            NodeState registry = system.getChildNode("jcr:nodeTypes");
            if (registry != null) {
                for (ChildNodeEntry child : registry.getChildNodeEntries(0, -1)) {
                    types.add(child.getName());
                }
            }
        }

        return new TypeValidator(types);
    }

}
