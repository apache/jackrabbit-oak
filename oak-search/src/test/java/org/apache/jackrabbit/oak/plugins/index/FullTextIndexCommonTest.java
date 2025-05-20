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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class FullTextIndexCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    @Test
    public void fullTextQuery() throws Exception {
        Tree index = setup(builder -> builder.indexRule("nt:base").property("propa").analyzed(), idx -> {
                },
                "propa");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello World!");
        test.addChild("b").setProperty("propa", "Simple test");
        test.addChild("c").setProperty("propa", "Hello everyone. This is a fulltext test");
        test.addChild("d").setProperty("propa", "howdy! hello again");
        root.commit();

        String query = "//*[jcr:contains(@propa, 'Hello')]";

        assertEventually(() -> {
            assertThat(explain(query, XPATH), containsString(indexOptions.getIndexType() + ":" + index.getName()));
            assertQuery(query, XPATH, List.of("/test/a", "/test/c", "/test/d"));
        });
    }

    @Test
    public void fullTextWithInvalidSyntax() throws Exception {
        Tree index = setup(builder -> builder.indexRule("nt:base").property("propa").analyzed(), idx -> {
                },
                "propa");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello everyone. This is a fulltext test");
        root.commit();

        // fuzziness support the following syntax: <term>~[edit_distance] (eg: hello~[similarity value]). The query below is invalid
        // https://lucene.apache.org/core/2_9_4/queryparsersyntax.html#Fuzzy%20Searches
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-fuzziness
        String query = "//*[jcr:contains(@propa, 'hello e~one')]";

        assertEventually(() -> {
            assertThat(explain(query, XPATH), containsString(indexOptions.getIndexType() + ":" + index.getName()));
            assertQuery(query, XPATH, List.of());
        });
    }

    @Test
    public void fullTextWithFuzziness() throws Exception {
        Tree index = setup(builder -> builder.indexRule("nt:base").property("propa").analyzed(), idx -> {
                },
                "propa");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello World!");
        test.addChild("b").setProperty("propa", "hello~folks!");
        test.addChild("c").setProperty("propa", "Hello everyone!");
        root.commit();

        String misspelledWorld = "//*[jcr:contains(@propa, 'wordl~0.5')]";
        String multipleMisspelledWorlds = "//*[jcr:contains(@propa, 'wordl~0.5 OR everone~0.5')]";
        String withTilde = "//*[jcr:contains(@propa, 'hello\\~folks')]";

        assertEventually(() -> {
            assertThat(explain(misspelledWorld, XPATH), containsString(indexOptions.getIndexType() + ":" + index.getName()));

            assertQuery(misspelledWorld, XPATH, List.of("/test/a"));
            assertQuery(multipleMisspelledWorlds, XPATH, List.of("/test/a", "/test/c"));
            assertQuery(withTilde, XPATH, List.of("/test/b"));
        });
    }

    @Test
    public void fullTextQueryRegExp() throws Exception {
        Tree index = setup(builder -> builder.indexRule("nt:base").property("propa").analyzed(), idx -> {
                },
                "propa");

        // test borrowed from: https://github.com/apache/lucene/issues/11537
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < 50000; i++) {
            strBuilder.append("b");
        }

        String query = "//*[rep:native('lucene', '/" + strBuilder + "/')]";

        assertEventually(() -> {
            assertThat(explain(query, XPATH), containsString(indexOptions.getIndexType() + ":" + index.getName()));
            assertQuery(query, XPATH, List.of());
        });
    }

    @Test
    public void fullTextQueryWithDifferentBoosts() throws Exception {
        setup(builder -> {
                    builder.indexRule("nt:base").property("propa").analyzed().nodeScopeIndex().boost(10);
                    builder.indexRule("nt:base").property("propb").analyzed().nodeScopeIndex().boost(100);
                }, idx -> {
                },
                "propa", "propb");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello World!");
        test.addChild("b").setProperty("propb", "Hello World");
        Tree c = test.addChild("c");
        c.setProperty("propa", "Hello people");
        c.setProperty("propb", "Hello folks");
        test.addChild("d").setProperty("propb", "baz");
        root.commit();
        for (String prefix : getPrefixes()) {
            assertEventually(() -> {
                assertQuery("//*[jcr:contains(., '" + prefix + "Hello')]", XPATH,
                    List.of("/test/c", "/test/b", "/test/a"), true, true);
                assertQuery("//*[jcr:contains(., '" + prefix + "Hello')] order by @jcr:score ascending", XPATH,
                    List.of("/test/a", "/test/b", "/test/c"), true, true);
                assertQuery("//*[jcr:contains(., '" + prefix + "people')]", XPATH, List.of("/test/c"));
            });
        }
    }

    @Test
    public void noStoredIndexDefinition() throws Exception {
        Tree index = setup(builder -> builder.indexRule("nt:base").property("propa").analyzed(), idx -> {
                },
                "propa");

        assertEventually(() -> {
            Tree indexNode = root.getTree("/" + INDEX_DEFINITIONS_NAME + "/" + index.getName());
            PropertyState ps = indexNode.getProperty(IndexConstants.REINDEX_COUNT);
            assertTrue(ps != null && ps.getValue(Type.LONG) == 1 && !indexNode.hasChild(INDEX_DEFINITION_NODE));
        });
    }

    /*
    In this test only nodeScope property is set over index. (OAK-9166)
     */
    @Test
    public void onlyNodeScopeIndexedQuery() throws Exception {
        setup(builder -> {
                    builder.indexRule("nt:base").property("a").nodeScopeIndex();
                    builder.indexRule("nt:base").property("b").nodeScopeIndex();
                }, idx -> {
                },
                "a", "b");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("nodea").setProperty("a", "hello");
        test.addChild("nodeb").setProperty("a", "world");
        test.addChild("nodec").setProperty("a", "hello world");
        Tree d = test.addChild("noded");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        for (String prefix : getPrefixes()) {
            assertEventually(() -> {
                assertQuery("//*[jcr:contains(., '" + prefix + "Hello')]", XPATH, List.of("/test/nodea", "/test/nodec", "/test/noded"));
                assertQuery("//*[jcr:contains(., '" + prefix + "hello world')]", XPATH, List.of("/test/nodec", "/test/noded"));
                assertQuery("//*[jcr:contains(., '" + prefix + "hello OR world')]", XPATH, List.of("/test/nodea", "/test/nodeb", "/test/nodec", "/test/noded"));
            });
        }
    }

    @Test
    public void nodeScopeIndexedQuery() throws Exception {
        setup(builder -> {
                    builder.indexRule("nt:base").property("a").analyzed().nodeScopeIndex();
                    builder.indexRule("nt:base").property("b").analyzed().nodeScopeIndex();
                }, idx -> {
                },
                "a", "b");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("a", "hello");
        test.addChild("b").setProperty("a", "world");
        test.addChild("c").setProperty("a", "hello world");
        Tree d = test.addChild("d");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        for (String prefix : getPrefixes()) {
            assertEventually(() -> {
                assertQuery("//*[jcr:contains(., '" + prefix + "Hello')]", XPATH, List.of("/test/a", "/test/c", "/test/d"));
                assertQuery("//*[jcr:contains(., '" + prefix + "hello world')]", XPATH, List.of("/test/c", "/test/d"));
            });
        }
    }

    @Test
    public void propertyIndexWithNodeScopeIndexedQuery() throws Exception {
        setup(builder -> {
                    builder.indexRule("nt:base").property("a").propertyIndex().nodeScopeIndex();
                    builder.indexRule("nt:base").property("b").propertyIndex().nodeScopeIndex();
                }, idx -> {
                },
                "a", "b");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("nodea").setProperty("a", "hello");
        test.addChild("nodeb").setProperty("a", "world");
        test.addChild("nodec").setProperty("a", "hello world");
        Tree d = test.addChild("noded");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        for (String prefix : getPrefixes()) {
            assertEventually(() -> {
                assertQuery("//*[jcr:contains(., '" + prefix + "Hello')]", XPATH, List.of("/test/nodea", "/test/nodec", "/test/noded"));
                assertQuery("//*[jcr:contains(., '" + prefix + "hello world')]", XPATH, List.of("/test/nodec", "/test/noded"));
            });
        }
    }

    /*
        In this test only we set nodeScope on a property and on b property just analyzed property is set over index. (OAK-9166)
        contains query of type contain(., 'string') should not return b.
     */
    @Test
    public void onlyAnalyzedPropertyShouldNotBeReturnedForNodeScopeIndexedQuery() throws Exception {
        setup(builder -> {
                    builder.indexRule("nt:base").property("a").nodeScopeIndex();
                    builder.indexRule("nt:base").property("b").analyzed();
                }, idx -> {
                },
                "a", "b");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("nodea").setProperty("b", "hello");
        test.addChild("nodeb").setProperty("b", "world");
        test.addChild("nodec").setProperty("a", "hello world");
        Tree d = test.addChild("noded");
        d.setProperty("a", "hello");
        d.setProperty("b", "world");
        root.commit();

        for (String prefix : getPrefixes()) {
            assertEventually(() -> {
                assertQuery("//*[jcr:contains(., '" + prefix + "Hello')]", XPATH, List.of("/test/nodec", "/test/noded"));
                assertQuery("//*[jcr:contains(., '" + prefix + "hello world')]", XPATH, List.of("/test/nodec"));
            });
        }
    }

    @Test
    public void fullTextMultiTermQuery() throws Exception {
        setup();

        //add content
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("analyzed_field", "test123");
        test.addChild("b").setProperty("analyzed_field", "test456");
        root.commit();

        assertEventually(() ->
                assertQuery("//*[jcr:contains(@analyzed_field, 'test123')]", XPATH, List.of("/test/a"))
        );
    }

    @Test
    public void fulltextWithModifiedNodeScopeIndex() throws Exception {
        Tree index = setup();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("analyzed_field", "sun.jpg");
        root.commit();

        assertEventually(() ->
                assertQuery("//*[jcr:contains(@analyzed_field, 'SUN.JPG')]", XPATH, List.of("/test/a")));

        // add nodeScopeIndex at a later stage
        index.getChild("indexRules").getChild("nt:base").getChild("properties")
                .getChild("analyzed_field").setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        index.setProperty(FulltextIndexConstants.PROP_REFRESH_DEFN, true);
        root.commit();

        for (String prefix : getPrefixes()) {
            assertEventually(() ->
                assertQuery("//*[jcr:contains(., '" + prefix + "jpg')]", XPATH, List.of("/test/a")));
        }
    }

    @Test
    public void fulltextWithMalformedFields() throws Exception {
        setup(builder -> {
            builder.indexRule("nt:base").property("string_field").type("String").analyzed().nodeScopeIndex();
            builder.indexRule("nt:base").property("date_field").type("Date").analyzed().nodeScopeIndex();
            builder.indexRule("nt:base").property("long_field").type("Long").analyzed().nodeScopeIndex();
            builder.indexRule("nt:base").property("double_field").type("Double").analyzed().nodeScopeIndex();
            builder.indexRule("nt:base").property("bool_field").type("Boolean").analyzed().nodeScopeIndex();
        }, idx -> {
        }, "string_field", "date_field", "long_field", "double_field", "bool_field");

        //add content
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("string_field", "foo");
        test.addChild("b").setProperty("date_field", "2025-bar");
        test.addChild("c").setProperty("long_field", "123-bar");
        test.addChild("d").setProperty("double_field", "456.78-bar");
        test.addChild("e").setProperty("bool_field", "true-bar");

        root.commit();

        for (String prefix : getPrefixes()) {
            assertEventually(() -> {
                    assertQuery("//*[jcr:contains(., '" + prefix + "foo')]", XPATH, List.of("/test/a"));
                    assertQuery("//*[jcr:contains(., '" + prefix + "2025')]", XPATH, List.of("/test/b"));
                    assertQuery("//*[jcr:contains(., '" + prefix + "123')]", XPATH, List.of("/test/c"));
                    assertQuery("//*[jcr:contains(., '" + prefix + "456.78')]", XPATH, List.of("/test/d"));
                    assertQuery("//*[jcr:contains(., '" + prefix + "true')]", XPATH, List.of("/test/e"));
                }
            );
        }
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    private static final BiConsumer<IndexDefinitionBuilder, List<String>> DEFAULT_BUILDER_HOOK = ((builder, analyzedFields) ->
            analyzedFields.forEach(f -> builder.indexRule("nt:base").property(f).analyzed()));

    protected Tree setup() throws Exception {
        return setup(List.of("analyzed_field"), idx -> {
        });
    }

    protected Tree setup(List<String> analyzedFields, Consumer<Tree> indexHook) throws Exception {
        return setup(
                builder -> DEFAULT_BUILDER_HOOK.accept(builder, analyzedFields),
                indexHook,
                analyzedFields.toArray(new String[0])
        );
    }

    protected Tree setup(Consumer<IndexDefinitionBuilder> builderHook, Consumer<Tree> indexHook, String... propNames) throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndex(
                indexOptions.createIndexDefinitionBuilder(), false, propNames);
        builder.noAsync();
        builder.evaluatePathRestrictions();
        builderHook.accept(builder);

        Tree index = indexOptions.setIndex(root, UUID.randomUUID().toString(), builder);
        indexHook.accept(index);
        root.commit();

        return index;
    }

    protected String explain(String query, String lang) {
        String explain = "explain " + query;
        return executeQuery(explain, lang).get(0);
    }

    protected String[] getPrefixes() {
        return new String[]{""};
    }
}
