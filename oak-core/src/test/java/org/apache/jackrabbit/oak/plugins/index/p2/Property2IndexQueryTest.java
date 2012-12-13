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
package org.apache.jackrabbit.oak.plugins.index.p2;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;

import com.google.common.collect.ImmutableList;

/**
 * Tests the query engine using the default index implementation: the
 * {@link Property2IndexProvider}
 */
public class Property2IndexQueryTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
                .with(new Property2IndexProvider())
                .with(new Property2IndexHookProvider())
                .createContentRepository();
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        Tree indexDef = index.addChild(INDEX_DEFINITIONS_NAME).addChild(
                TEST_INDEX_NAME);
        indexDef.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        indexDef.setProperty(TYPE_PROPERTY_NAME, Property2Index.TYPE);
        indexDef.setProperty(REINDEX_PROPERTY_NAME, true);
        indexDef.setProperty(PropertyStates
                .createProperty("propertyNames",
                        ImmutableList.of(JCR_PRIMARYTYPE, JCR_MIXINTYPES),
                        Type.STRINGS));
        root.commit();
    }

}