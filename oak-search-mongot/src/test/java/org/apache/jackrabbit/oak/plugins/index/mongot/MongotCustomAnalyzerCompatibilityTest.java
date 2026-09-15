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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.List;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongotCustomAnalyzerCompatibilityTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void composedAnalyzerRunsInMongot() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        IndexDefinitionBuilder definition = builder.definition();
        definition.indexRule("nt:base").property("text").analyzed().nodeScopeIndex();

        Tree analyzer = definition.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT);
        analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

        Tree charFilters = analyzer.addChild(FulltextIndexConstants.ANL_CHAR_FILTERS);
        charFilters.setOrderableChildren(true);
        Tree mapping = charFilters.addChild("Mapping");
        mapping.setProperty("mapping", "digits.txt");
        mapping.addChild("digits.txt").addChild("jcr:content").setProperty("jcr:data",
                "\"٢\" => \"2\"\n\"٥\" => \"5\"\n\"٠\" => \"0\"\n\"١\" => \"1\"");

        Tree filters = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS);
        filters.setOrderableChildren(true);
        filters.addChild("LowerCase");
        Tree stop = filters.addChild("Stop");
        stop.setProperty("words", "stop.txt");
        stop.addChild("stop.txt").addChild("jcr:content").setProperty("jcr:data", "my\nis");
        filters.addChild("PorterStem");

        NodeBuilder content = builder.root().child("content");
        content.child("mapped").setProperty("text", "My license plate is ٢٥٠١٥");
        content.child("stemmed").setProperty("text", "Foxes jumping quickly");

        try (MongotTestRepositoryBuilder.Fixture repository = builder.build()) {
            String select = "select [jcr:path] from [nt:base] where contains(*, '%s')";
            assertEquals(List.of("/content/mapped"), repository.paths(
                    String.format(select, "25015"), "JCR-SQL2"));
            assertEquals(List.of("/content/stemmed"), repository.paths(
                    String.format(select, "jump"), "JCR-SQL2"));
        }
    }
}
