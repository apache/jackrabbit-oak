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
import java.util.Collections;
import java.util.UUID;

public class ElasticDynamicBoostQueryTest extends ElasticAbstractQueryTest {

    public static final String ASSET_NODE_TYPE =
            "[dam:Asset]\n" +
                    " - * (UNDEFINED) multiple\n" +
                    " - * (UNDEFINED)\n" +
                    " + * (nt:base) = oak:TestNode VERSION";

    private static final String UNSTRUCTURED = "nt:unstructured";

    @Test
    public void dynamicBoost() throws CommitFailedException {
        NodeTypeRegistry.register(root, toInputStream(ASSET_NODE_TYPE), "test nodeType");
        IndexDefinitionBuilder builder = createIndex(true, "dam:Asset", "title", "dynamicBoost");
        IndexDefinitionBuilder.PropertyRule title = builder.indexRule("dam:Asset")
                .property("title")
                .analyzed();
        title.getBuilderTree().setProperty(JcrConstants.JCR_PRIMARYTYPE, UNSTRUCTURED, Type.NAME);
        IndexDefinitionBuilder.PropertyRule db = builder.indexRule("dam:Asset").property("dynamicBoost");
        Tree dbTree = db.getBuilderTree();
        dbTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, UNSTRUCTURED, Type.NAME);
        dbTree.setProperty("name", "jcr:content/metadata/.*");
        dbTree.setProperty("isRegexp", true);
        dbTree.setProperty("dynamicBoost", true);
        setIndex("damAsset_" + UUID.randomUUID(), builder);
        root.commit();

        Tree test = createNodeWithType(root.getTree("/"), "test", UNSTRUCTURED);
        Tree item1 = createNodeWithType(test, "item1", "dam:Asset");
        item1.setProperty("title", "flower with a lot of red and a bit of blue");

        Tree item1Metadata = createNodeWithType(
                createNodeWithType(item1, JcrConstants.JCR_CONTENT, UNSTRUCTURED),
                "metadata", UNSTRUCTURED);
        Tree item1Color1 = createNodeWithType(item1Metadata,"color1", UNSTRUCTURED);
        item1Color1.setProperty("name", "red");
        item1Color1.setProperty("confidence", 9.0);
        Tree item1Color2 = createNodeWithType(item1Metadata,"color2", UNSTRUCTURED);
        item1Color2.setProperty("name", "blue");
        item1Color2.setProperty("confidence", 1.0);

        Tree item2 = createNodeWithType(test, "item2", "dam:Asset");
        item2.setProperty("title", "flower with a lot of blue and a bit of red");
        Tree item2Metadata = createNodeWithType(
                createNodeWithType(item2, JcrConstants.JCR_CONTENT, UNSTRUCTURED),
                "metadata", UNSTRUCTURED);
        Tree item2Color1 = createNodeWithType(item1Metadata,"color1", UNSTRUCTURED);
        item2Color1.setProperty("name", "blue");
        item2Color1.setProperty("confidence", 9.0);
        Tree item2Color2 = createNodeWithType(item1Metadata,"color2", UNSTRUCTURED);
        item2Color2.setProperty("name", "red");
        item2Color2.setProperty("confidence", 1.0);
        root.commit();

        assertEventually(() -> {
            assertQuery("//element(*, dam:Asset)[jcr:contains(@title, 'flower')] ",
                    XPATH, Arrays.asList("/test/item1", "/test/item2"));
            assertQuery("//element(*, dam:Asset)[jcr:contains(@title, 'red flower')] ",
                    XPATH, Arrays.asList("/test/item1", "/test/item2"), true);
            assertQuery("//element(*, dam:Asset)[jcr:contains(@title, 'blue flower')] ",
                    XPATH, Arrays.asList("/test/item2", "/test/item1"), true);
        });
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
