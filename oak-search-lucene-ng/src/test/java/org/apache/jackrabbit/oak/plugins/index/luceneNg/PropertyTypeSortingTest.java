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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.jackrabbit.oak.plugins.index.luceneNg.JcrValue.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * How {@code order by} sorts an ordered property for a given declared index type. Each case lists
 * the values in the expected ascending order; a multi-valued property sorts by its smallest value.
 */
public class PropertyTypeSortingTest {

    static Stream<Arguments> sortCases() {
        return Stream.of(
                ascending(Type.UNDEFINED, jcrString("a"), jcrStrings("b", "z"), jcrString("c")),
                ascending(Type.UNDEFINED, jcrLong(10L), jcrLong(2L)),
                ascending(Type.UNDEFINED, jcrDate("2026-01-01T00:00:00.000Z"), jcrDate("2026-06-01T00:00:00.000Z")),
                ascending(Type.UNDEFINED, jcrPath("/a"), jcrPaths("/b", "/z"), jcrPath("/c")),
                ascending(Type.UNDEFINED, jcrDecimal("10.5"), jcrDecimal("2")),

                ascending(Type.STRING, jcrString("a"), jcrStrings("b", "z"), jcrString("c")),
                ascending(Type.STRING, jcrDecimal("1.5"), jcrString("a")),

                ascending(Type.LONG, jcrLong(1L), jcrLongs(2L, 30L), jcrLong(5L)),
                ascending(Type.LONG, jcrString("2"), jcrLong(10L)),

                ascending(Type.DOUBLE, jcrDouble(1.0), jcrDoubles(2.0, 30.0), jcrDouble(5.0)),
                ascending(Type.DOUBLE, jcrDouble(-1.5), jcrDouble(2.5), jcrDouble(10.0)),

                ascending(Type.DATE, jcrDate("2026-01-01T00:00:00.000Z"),
                        jcrDates("2026-02-01T00:00:00.000Z", "2026-12-01T00:00:00.000Z"),
                        jcrDate("2026-06-01T00:00:00.000Z")),

                ascending(Type.DECIMAL, jcrDecimal("1.5"), jcrDecimals("2", "30"), jcrDecimal("10.25")),

                ascending(Type.BOOLEAN, jcrBooleans(false, true), jcrBoolean(true)),

                ascending(Type.NAME, jcrName("nt:a"), jcrNames("nt:b", "nt:z"), jcrName("nt:c")),

                ascending(Type.PATH, jcrPath("/a"), jcrPaths("/b", "/z"), jcrPath("/c")),

                ascending(Type.REFERENCE, jcrReference("00000000-0000-4000-8000-000000000001"),
                        jcrReferences("00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000009"),
                        jcrReference("00000000-0000-4000-8000-000000000003")),

                ascending(Type.WEAKREFERENCE, jcrWeakReference("00000000-0000-4000-8000-000000000001"),
                        jcrWeakReferences("00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000009"),
                        jcrWeakReference("00000000-0000-4000-8000-000000000003")),

                ascending(Type.URI, jcrUri("https://a.example"),
                        jcrUris("https://b.example", "https://z.example"), jcrUri("https://c.example")));
    }

    @ParameterizedTest(name = "declared {0}, ascending {1}")
    @MethodSource("sortCases")
    public void ordersByDeclaredType(Type<?> declaredType, List<JcrValue> ascendingValues) throws Exception {
        Root root = newRepositoryRoot();
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder().noAsync();
        IndexDefinitionBuilder.PropertyRule property = builder.indexRule("nt:base")
                .property("value").propertyIndex().ordered();
        if (declaredType != Type.UNDEFINED) {
            property.type(PropertyType.nameFromValue(declaredType.tag()));
        }
        builder.build(root.getTree("/oak:index").addChild("test")).setProperty("type", "luceneNg");
        Tree content = root.getTree("/").addChild("content");
        List<String> expectedPaths = new ArrayList<>();
        for (int i = 0; i < ascendingValues.size(); i++) {
            expectedPaths.add("/content/node" + i);
        }
        for (int i = ascendingValues.size() - 1; i >= 0; i--) {
            content.addChild("node" + i).setProperty(ascendingValues.get(i).property("value"));
        }
        root.commit();

        assertEquals(expectedPaths, query(root,
                "select [jcr:path] from [nt:base] where [value] is not null order by [value] option(index name test)"));
    }

    private static Arguments ascending(Type<?> declaredType, JcrValue... ascendingValues) {
        return Arguments.of(declaredType, List.of(ascendingValues));
    }

    private static Root newRepositoryRoot() throws Exception {
        QueryEngineSettings settings = new QueryEngineSettings();
        settings.setFailTraversal(true);
        DefaultWhiteboard whiteboard = new DefaultWhiteboard();
        whiteboard.register(QueryEngineSettings.class, settings, Map.of());
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        LuceneNgQueryIndexProvider queryProvider = new LuceneNgQueryIndexProvider(tracker);
        MemoryNodeStore store = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        store.addObserver(queryProvider);
        return new Oak(store)
                .with(new OpenSecurityProvider())
                .with(whiteboard)
                .with((QueryIndexProvider) queryProvider)
                .with(new LuceneNgIndexEditorProvider(tracker))
                .createContentRepository()
                .login(null, null)
                .getLatestRoot();
    }

    private static List<String> query(Root root, String sql) throws Exception {
        List<String> paths = new ArrayList<>();
        for (ResultRow row : root.getQueryEngine().executeQuery(sql, "sql", Optional.empty(), Optional.empty(),
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS).getRows()) {
            paths.add(row.getPath());
        }
        return paths;
    }
}
