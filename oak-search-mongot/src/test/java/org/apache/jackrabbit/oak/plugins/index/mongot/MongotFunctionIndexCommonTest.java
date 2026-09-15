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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.FunctionIndexCommonTest;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongotIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.ClassRule;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

public class MongotFunctionIndexCommonTest extends FunctionIndexCommonTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    public MongotFunctionIndexCommonTest() {
        indexOptions = new MongotIndexOptions();
    }

    @Override
    protected String getIndexProvider() {
        return "mongot:";
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new MongotCommonTestRepositoryBuilder(mongo).build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected Tree createIndex(String name, Set<String> propertyNames) {
        return createIndex(root.getTree("/"), name, propertyNames);
    }

    @Override
    protected Tree createIndex(Tree parent, String name, Set<String> propertyNames) {
        Tree definition = parent.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        definition.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        definition.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        definition.setProperty(REINDEX_PROPERTY_NAME, true);
        definition.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        definition.setProperty(PropertyStates.createProperty(
                FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propertyNames, Type.STRINGS));
        return parent.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    @Override
    protected String getLoggerName() {
        return MongotIndexEditorProvider.class.getName();
    }
}
