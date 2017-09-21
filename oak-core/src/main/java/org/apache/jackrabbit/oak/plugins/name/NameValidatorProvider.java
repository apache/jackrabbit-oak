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

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NAMESPACES;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NSDATA;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_PREFIXES;

/**
 * Validator service that checks that all node and property names as well
 * as any name values are syntactically valid and that any namespace prefixes
 * are properly registered.
 */
@Component
@Service(EditorProvider.class)
public class NameValidatorProvider extends ValidatorProvider {

    @Override
    public Validator getRootValidator(
            NodeState before, NodeState after, CommitInfo info) {
        return new NameValidator(newHashSet(after
                .getChildNode(JCR_SYSTEM)
                .getChildNode(REP_NAMESPACES)
                .getChildNode(REP_NSDATA)
                .getStrings(REP_PREFIXES)));
    }

}
