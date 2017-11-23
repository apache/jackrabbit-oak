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
package org.apache.jackrabbit.oak.plugins.index.property;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;
import static org.junit.Assert.assertTrue;

import javax.jcr.query.Query;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

public class ValuePatternPropertyIndexTests extends AbstractQueryTest {
    private static final String INDEXED_PROPERTY = "indexedProperty";
    
    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new PropertyIndexProvider())
            .with(new PropertyIndexEditorProvider())
            .createContentRepository();
    }

    @Test
    public void valuePattern() throws Exception {
//        valuePattern("", "* property test-index");
//        valuePattern("x", "* property test-index");
        valuePattern(" ", "* property test-index");
        valuePattern("-", "* property test-index");
        valuePattern("/", "* property test-index");
    }
    
    private void valuePattern(String middle, String plan) throws Exception {
        Tree index = super.createTestIndexNode(root.getTree("/"), TYPE);
        index.setProperty(PROPERTY_NAMES, of(INDEXED_PROPERTY), NAMES);
        index.setProperty(IndexConstants.VALUE_INCLUDED_PREFIXES, "hello" + middle + "world");
        index.setProperty(REINDEX_PROPERTY_NAME, true);
        root.commit();
        Tree content = root.getTree("/").addChild("content");
        content.addChild("node1").setProperty(INDEXED_PROPERTY, "hello" + middle + "world");
        content.addChild("node2").setProperty(INDEXED_PROPERTY, "hello");
        root.commit();
        String statement = "explain select * from [nt:base] where [" + INDEXED_PROPERTY + "] = 'hello" + middle + "world'";
        String result = executeQuery(statement, Query.JCR_SQL2, false, false).toString();
        assertTrue(result, result.indexOf(plan) >= 0);
    }
    
}
