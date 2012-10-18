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
package org.apache.jackrabbit.oak.plugins.nodetype;

import java.util.Map;

import javax.jcr.nodetype.NodeTypeTemplate;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;

class DefBuilderFactory extends
        DefinitionBuilderFactory<NodeTypeTemplate, Map<String, String>> {

    private final Tree root;

    public DefBuilderFactory(Tree root) {
        this.root = root;
    }

    @Override
    public NodeTypeTemplateImpl newNodeTypeDefinitionBuilder() {
        return new NodeTypeTemplateImpl(new NameMapperImpl(root));
    }

    @Override
    public Map<String, String> getNamespaceMapping() {
        return Namespaces.getNamespaceMap(root);
    }

    @Override
    public void setNamespaceMapping(Map<String, String> namespaces) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNamespace(String prefix, String uri) {
        if (Namespaces.getNamespaceMap(root).containsValue(uri)) {
            return; // namespace already exists
        }

        Tree namespaces = getOrCreate(
                JcrConstants.JCR_SYSTEM, NamespaceConstants.REP_NAMESPACES);
        namespaces.setProperty(prefix, uri);
    }

    private Tree getOrCreate(String... path) {
        Tree tree = root;
        for (String name : path) {
            Tree child = tree.getChild(name);
            if (child == null) {
                child = tree.addChild(name);
            }
            tree = child;
        }
        return tree;
    }

}
