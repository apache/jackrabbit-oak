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
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;

import java.util.List;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

public class SinglePropertyIndexQueryTests extends AbstractQueryTest {
    private static final String INDEXED_PROPERTY = "indexedProperty";
    
    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new PropertyIndexProvider())
            .with(new PropertyIndexEditorProvider())
            .createContentRepository();
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = super.createTestIndexNode(root.getTree("/"), TYPE);
        index.setProperty(PROPERTY_NAMES, of(INDEXED_PROPERTY), NAMES);
        index.setProperty(REINDEX_PROPERTY_NAME, true);
        root.commit();
    }

    @Test
    public void oak2146() throws Exception {
        final String statement = "//*[ " +
            "(@" + INDEXED_PROPERTY + " = 'a' or @" + INDEXED_PROPERTY + " = 'c') " +
            "and " +
            "(@" + INDEXED_PROPERTY + " = 'b' or @" + INDEXED_PROPERTY + " = 'd')]";
        List<String> expected = newArrayList();
        
        Tree content = root.getTree("/").addChild("content");
        
        // adding /content/node1 { a, b } 
        Tree node = content.addChild("node1");
        node.setProperty(INDEXED_PROPERTY, of("a", "b"), STRINGS);
        expected.add(node.getPath());
        
        // adding /content/node2 { c, d }
        node = content.addChild("node2");
        node.setProperty(INDEXED_PROPERTY, of("c", "d"), STRINGS);
        expected.add(node.getPath());
        
        // adding nodes with {a, x} and {c, x} these should not be returned
        node = content.addChild("node3");
        node.setProperty(INDEXED_PROPERTY, of("a", "x"), STRINGS);
        node = content.addChild("node4");
        node.setProperty(INDEXED_PROPERTY, of("c", "x"), STRINGS);
        
        root.commit();
        
        assertQuery(statement, "xpath", expected);
    }
}
