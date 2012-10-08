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

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.namepath.NameMapperImpl;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

@Component
@Service(ValidatorProvider.class)
public class TypeValidatorProvider implements ValidatorProvider {

    @Override
    public Validator getRootValidator(NodeState before, final NodeState after) {
        ReadOnlyNodeTypeManager ntm = new ReadOnlyNodeTypeManager() {
            private final Tree types = getTypes(after);

            @Override
            protected Tree getTypes() {
                return types;
            }

            private Tree getTypes(NodeState after) {
                Tree tree = new ReadOnlyTree(after);
                for (String name : PathUtils.elements(NODE_TYPES_PATH)) {
                    if (tree == null) {
                        break;
                    }
                    else {
                        tree = tree.getChild(name);
                    }
                }
                return tree;
            }
        };

        Tree root = new ReadOnlyTree(after);
        final NamePathMapper mapper = new NamePathMapperImpl(new NameMapperImpl(root));
        return new TypeValidator(ntm, new ReadOnlyTree(after), mapper);
    }

}
