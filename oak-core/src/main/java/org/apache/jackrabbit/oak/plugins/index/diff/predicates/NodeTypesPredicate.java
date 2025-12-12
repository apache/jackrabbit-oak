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
package org.apache.jackrabbit.oak.plugins.index.diff.predicates;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.index.optimizer.FulltextIndexConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NodeTypesPredicate implements Predicate<JsonObject> {

    private static final Logger LOG = LoggerFactory.getLogger(NodeTypesPredicate.class);

    private final Set<String> nodeTypes;

    public NodeTypesPredicate(final Set<String> nodeTypes) {
        this.nodeTypes = nodeTypes;
    }

    @Override
    public boolean test(final JsonObject indexJson) {
        if (indexJson.getChildren().containsKey(FulltextIndexConstants.INDEX_RULES)) {
            final JsonObject indexRules = indexJson.getChildren().get(FulltextIndexConstants.INDEX_RULES);

            final Set<String> indexNodeTypes = indexRules.getChildren().keySet()
                .stream()
                .filter(name -> !name.equals(JcrConstants.JCR_PRIMARYTYPE))
                .collect(Collectors.toSet());

            LOG.debug("Generated index node types: {}, candidate index node types: {}", nodeTypes, indexNodeTypes);

            // Existing index must include all the node types in the generated index
            return indexNodeTypes.containsAll(nodeTypes);
        } else {
            LOG.debug("Candidate index has no index rules, skipping: {}", indexJson);

            return false;
        }
    }
}
