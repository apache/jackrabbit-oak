/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.UUID;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;

public class ElasticDynamicBoostQueryTest extends ElasticAbstractQueryTest {

    public static final String ASSET_NODE_TYPE =
            "[dam:Asset]\n" +
                    " - * (UNDEFINED) multiple\n" +
                    " - * (UNDEFINED)\n" +
                    " + * (nt:base) = oak:TestNode VERSION";

    @Test
    public void dynamicBoost() throws CommitFailedException {
        configureIndex();

        Tree test = createNodeWithType(root.getTree("/"), "test", NT_UNSTRUCTURED);
        Tree item1Metadata = createNodeWithMetadata(test, "item1", "flower with a lot of red and a bit of blue");
        Tree item1Color1 = createNodeWithType(item1Metadata,"color1", NT_UNSTRUCTURED);
        item1Color1.setProperty("name", "red");
        item1Color1.setProperty("confidence", 9.0);
        Tree item1Color2 = createNodeWithType(item1Metadata,"color2", NT_UNSTRUCTURED);
        item1Color2.setProperty("name", "blue");
        item1Color2.setProperty("confidence", 1.0);

        Tree item2Metadata = createNodeWithMetadata(test, "item2", "flower with a lot of blue and a bit of red");
        Tree item2Color1 = createNodeWithType(item2Metadata,"color1", NT_UNSTRUCTURED);
        item2Color1.setProperty("name", "blue");
        item2Color1.setProperty("confidence", 9.0);
        Tree item2Color2 = createNodeWithType(item2Metadata,"color2", NT_UNSTRUCTURED);
        item2Color2.setProperty("name", "red");
        item2Color2.setProperty("confidence", 1.0);
        root.commit();

        assertEventually(() -> {
            assertQuery("//element(*, dam:Asset)[jcr:contains(@title, 'flower')]",
                    XPATH, Arrays.asList("/test/item1", "/test/item2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(title, 'red flower')",
                    Arrays.asList("/test/item1", "/test/item2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(title, 'blue flower')",
                    Arrays.asList("/test/item2", "/test/item1"));
        });
    }

    @Test
    public void dynamicBoostAnalyzed() throws CommitFailedException {
        configureIndex();

        Tree test = createNodeWithType(root.getTree("/"), "test", NT_UNSTRUCTURED);
        Tree item1Metadata = createNodeWithMetadata(test, "item1", "flower with a lot of red and a bit of blue");
        item1Metadata.setProperty("foo", "bar");
        Tree item1Color1 = createNodeWithType(item1Metadata,"color1", NT_UNSTRUCTURED);
        item1Color1.setProperty("name", "red");
        item1Color1.setProperty("confidence", 9.0);
        Tree item1Color2 = createNodeWithType(item1Metadata,"color2", NT_UNSTRUCTURED);
        item1Color2.setProperty("name", "blue");
        item1Color2.setProperty("confidence", 1.0);

        Tree item2Metadata = createNodeWithMetadata(test, "item2", "flower with a lot of blue and a bit of red");
        Tree item2Color1 = createNodeWithType(item2Metadata,"color1", NT_UNSTRUCTURED);
        item2Color1.setProperty("name", "blue");
        item2Color1.setProperty("confidence", 9.0);
        Tree item2Color2 = createNodeWithType(item2Metadata,"color2", NT_UNSTRUCTURED);
        item2Color2.setProperty("name", "red");
        item2Color2.setProperty("confidence", 1.0);
        root.commit();

        assertEventually(() -> {
            assertQuery("//element(*, dam:Asset)[jcr:contains(@title, 'flower')]",
                    XPATH, Arrays.asList("/test/item1", "/test/item2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(title, 'red-flower')",
                    Arrays.asList("/test/item1", "/test/item2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(title, 'blue-flower')",
                    Arrays.asList("/test/item2", "/test/item1"));
        });
    }

    private void configureIndex() throws CommitFailedException {
        NodeTypeRegistry.register(root, toInputStream(ASSET_NODE_TYPE), "test nodeType");
        IndexDefinitionBuilder builder = createIndex(true, "dam:Asset", "title", "dynamicBoost");
        IndexDefinitionBuilder.PropertyRule title = builder.indexRule("dam:Asset")
                .property("title")
                .analyzed();
        title.getBuilderTree().setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        IndexDefinitionBuilder.PropertyRule db = builder.indexRule("dam:Asset").property("dynamicBoost");
        Tree dbTree = db.getBuilderTree();
        dbTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        dbTree.setProperty("name", "jcr:content/metadata/.*");
        dbTree.setProperty("isRegexp", true);
        dbTree.setProperty("dynamicBoost", true);
        setIndex("damAsset_" + UUID.randomUUID(), builder);
        root.commit();
    }

    private Tree createNodeWithMetadata(Tree parent, String nodeName, String title) {
        Tree item = createNodeWithType(parent, nodeName, "dam:Asset");
        item.setProperty("title", title);

        return createNodeWithType(
                createNodeWithType(item, JcrConstants.JCR_CONTENT, NT_UNSTRUCTURED),
                "metadata", NT_UNSTRUCTURED);
    }

    private static Tree createNodeWithType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    private static InputStream toInputStream(String x) {
        return new ByteArrayInputStream(x.getBytes());
    }
}
