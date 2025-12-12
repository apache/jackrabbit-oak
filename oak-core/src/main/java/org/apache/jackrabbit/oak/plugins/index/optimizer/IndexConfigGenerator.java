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
package org.apache.jackrabbit.oak.plugins.index.optimizer;

import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.optimizer.IndexDefinitionBuilder.IndexRule;
import org.apache.jackrabbit.oak.plugins.index.optimizer.IndexDefinitionBuilder.PropertyRule;
import org.apache.jackrabbit.oak.query.ExecutionContext;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class IndexConfigGenerator {

    private final QueryEngine queryEngine;
    private final IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    private final Set<String> propsWithFulltextConstraints = new HashSet<>();

    public IndexConfigGenerator() {
        final Root root = new ImmutableRoot(InitialContentHelper.INITIAL_CONTENT);
        queryEngine = new QueryEngineImpl() {
            @Override
            protected ExecutionContext getExecutionContext() {
                return new ExecutionContext(
                    InitialContentHelper.INITIAL_CONTENT,
                    root,
                    new QueryEngineSettings(),
                    new LuceneIndexGeneratingIndexProvider(), null, null) {
                    @Override
                    public NodeTypeInfoProvider getNodeTypeInfoProvider() {
                        return DummyNodeTypeInfoProvider.INSTANCE;
                    }
                };
            }
        };
    }

    public static boolean isXPath(String query) {
        // the query is not, at least SQL is not
        query = query.trim().toLowerCase(Locale.ENGLISH);
        // explain queries
        if (query.startsWith("explain")) {
            query = query.substring("explain".length()).trim();
            if (query.startsWith("measure")) {
                query = query.substring("measure".length()).trim();
            }
        }
        // union queries
        while (query.startsWith("(")) {
            query = query.substring("(".length()).trim();
        }

        return !query.startsWith("select");
    }

    public void process(String statement) throws ParseException {
        String lang = isXPath(statement) ? "xpath" : "JCR-SQL2";
        process(statement, lang);
    }

    public void process(String statement, String language) throws ParseException {
        queryEngine.executeQuery(statement, language, null, null);
    }

    public NodeState getIndexConfig() {
        return builder.build();
    }

    private void processFilter(Filter filter, List<OrderEntry> sortOrder) {
        boolean xpath = isOriginallyXPath(filter.getQueryStatement());
        addPathRestrictions(filter);
        IndexRule rule = processNodeTypeConstraint(filter);
        processTags(filter);
        processFulltextConstraints(filter, rule);
        processPropertyRestrictions(filter, rule);
        processSortConditions(sortOrder, rule, xpath);
        processPureNodeTypeConstraints(filter, rule);
    }

    private void processTags(Filter filter) {
        PropertyRestriction indexTag = filter.getPropertyRestriction(IndexConstants.INDEX_TAG_OPTION);

        if (indexTag != null && indexTag.first != null) {
            builder.tags(indexTag.first.getValue(Type.STRING));
        }
    }

    private void addPathRestrictions(Filter filter) {
        if (!filter.getPath().isEmpty() && !"/".equals(filter.getPath())) {
            String path = filter.getPath().replaceAll("\\s", "");
            builder.includedPaths(path);
            builder.queryPaths(path);
        }
    }

    private void processPureNodeTypeConstraints(Filter filter, IndexRule rule) {
        if (filter.getFullTextConstraint() == null
            && filter.getPropertyRestrictions().isEmpty()
            && !"nt:base".equals(filter.getNodeType())) {
            rule.property("jcr:primaryType");
        }
    }

    private void processFulltextConstraints(Filter filter, final IndexRule rule) {
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            return;
        }

        ft.accept(new FullTextVisitor.FullTextVisitorBase() {
            @Override
            public boolean visit(FullTextContains contains) {
                visitTerm(contains.getPropertyName());
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                visitTerm(term.getPropertyName());
                return false;
            }

            private void visitTerm(String propertyName) {
                String p = propertyName;
                String propertyPath = null;
                String nodePath = null;
                if (p == null) {
                    return;
                }
                String parent = getParentPath(p);
                if (isNodePath(p)) {
                    nodePath = parent;
                } else {
                    propertyPath = p;
                }

                if (nodePath != null) {
                    builder.aggregateRule(rule.getRuleName()).include(nodePath).relativeNode();
                } else if (propertyPath != null) {
                    rule.property(propertyPath).analyzed();
                    propsWithFulltextConstraints.add(propertyPath);
                }
            }
        });
    }

    /**
     * In a fulltext term for jcr:contains(foo, 'bar') 'foo' is the property name. While in
     * jcr:contains(foo/*, 'bar') 'foo' is node name
     *
     * @return true if the term is related to node
     */
    private static boolean isNodePath(String fulltextTermPath) {
        return fulltextTermPath.endsWith("/*");
    }

    private void processSortConditions(List<OrderEntry> sortOrder, IndexRule rule, boolean isXPath) {
        if (sortOrder == null) {
            return;
        }

        for (OrderEntry o : sortOrder) {
            if ("jcr:score".equals(o.getPropertyName())) {
                continue;
            }

            if (o.getPropertyType().isArray()) {
                continue;
            }

            String propertyName = o.getPropertyName();
            if (isFunction(propertyName)) {
                String queryFunc = PolishToQueryConverter.apply(propertyName, isXPath);
                propertyName = FunctionNameConverter.apply(propertyName, isXPath);
                PropertyRule prop = rule.property(propertyName);
                prop.function(queryFunc);
                prop.ordered();
                continue;
            }

            PropertyRule propRule = rule.property(o.getPropertyName());
            if (o.getPropertyType() != Type.UNDEFINED) {
                propRule.ordered(PropertyType.nameFromValue(o.getPropertyType().tag()));
            } else {
                propRule.ordered();
            }
        }
    }

    /**
     * Returns if the propertyName is a function. If it is, it will be in Polish notation.
     *
     * @param propertyName the propertyName in a propertyRestriction
     * @return true if it is a function and false otherwise.
     */
    private boolean isFunction(String propertyName) {
        return propertyName.startsWith("function*");
    }


    /**
     * Returns if the query originally was written in XPath. When the query engine creates the
     * filter, the query statement is automatically SQL-2. But if it was originally XPath, it
     * contains a comment with the original XPath query.
     * <p>
     * Detecting it like this is only a heuristic. It is not 100% accurate as a JCR-SQL2 query might
     * contain a condition with this String literal. But in most cases, this should correctly detect
     * it.
     *
     * @param query the query statement
     * @return true if the query was originally XPath and false otherwise.
     */
    public static boolean isOriginallyXPath(String query) {
        return query.contains("/* xpath: ") && query.endsWith(" */");
    }

    private void processPropertyRestrictions(Filter filter, IndexRule rule) {
        System.out.println(filter.getQueryStatement());
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            //Ignore special restrictions
            if (isSpecialRestriction(pr)) {
                continue;
            }

            //QueryEngine adds a synthetic constraint for those properties
            //which are used in fulltext constraint so as to ensure that given
            //property is present. They need not be backed by index
            if (propsWithFulltextConstraints.contains(pr.propertyName)) {
                continue;
            }

            if (isFunction(pr.propertyName)) {
                boolean isXPath = isOriginallyXPath(filter.getQueryStatement());
                String queryFunc = PolishToQueryConverter.apply(pr.propertyName, isXPath);
                String propertyName = FunctionNameConverter.apply(pr.propertyName, isXPath);
                PropertyRule prop = rule.property(propertyName);
                prop.function(queryFunc);
                continue;
            }

            PropertyRule propRule = rule.property(pr.propertyName);
            if (pr.isNullRestriction()) {
                propRule.nullCheckEnabled();
            } else if (pr.isNotNullRestriction()) {
                propRule.notNullCheckEnabled();
            }
            propRule.propertyIndex();
        }

        if (filter.getPropertyRestriction(QueryConstants.RESTRICTION_LOCAL_NAME) != null) {
            rule.indexNodeName();
        }
    }

    private boolean isSpecialRestriction(PropertyRestriction pr) {
        String name = pr.propertyName;
        if (name.startsWith(":")) {
            return true;
        }
        if (name.startsWith("native*")) {
            return true;
        }
        return false;
    }

    private void processPathRestriction(Filter filter) {
        if (filter.getPathRestriction() != PathRestriction.NO_RESTRICTION
            || (filter.getPathRestriction() == PathRestriction.ALL_CHILDREN
            && !PathUtils.denotesRoot(filter.getPath()))
        ) {
            builder.evaluatePathRestrictions();
        }
    }

    private IndexRule processNodeTypeConstraint(Filter filter) {
        return builder.indexRule(filter.getNodeType());
    }

    private class LuceneIndexGeneratingIndexProvider implements QueryIndexProvider {

        @Override
        public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return List.of(new LuceneIndexGeneratingIndex());
        }
    }

    private class LuceneIndexGeneratingIndex implements QueryIndex.AdvancedQueryIndex, QueryIndex {

        @Override
        public double getMinimumCost() {
            return 1.0;
        }

        @Override
        public double getCost(Filter filter, NodeState nodeState) {
            return Double.MAX_VALUE;
        }

        @Override
        public Cursor query(Filter filter, NodeState nodeState) {
            return null;
        }

        @Override
        public String getPlan(Filter filter, NodeState nodeState) {
            return null;
        }

        @Override
        public String getIndexName() {
            return "LuceneIndexGenerator";
        }

        @Override
        public List<QueryIndex.IndexPlan> getPlans(Filter filter,
            List<OrderEntry> sortOrder, NodeState rootState) {
            processFilter(filter, sortOrder);
            return Collections.emptyList();
        }

        @Override
        public String getPlanDescription(QueryIndex.IndexPlan plan, NodeState root) {
            return null;
        }

        @Override
        public Cursor query(QueryIndex.IndexPlan plan, NodeState rootState) {
            return null;
        }
    }

    private enum DummyNodeTypeInfoProvider implements NodeTypeInfoProvider {
        INSTANCE;

        @Override
        public NodeTypeInfo getNodeTypeInfo(String nodeTypeName) {
            return new DummyNodeTypeInfo(nodeTypeName);
        }
    }

    private static class DummyNodeTypeInfo implements NodeTypeInfo {

        private final String nodeTypeName;

        private DummyNodeTypeInfo(String nodeTypeName) {
            this.nodeTypeName = nodeTypeName;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public String getNodeTypeName() {
            return nodeTypeName;
        }

        @Override
        public Set<String> getSuperTypes() {
            return new HashSet<>();
        }

        @Override
        public Set<String> getPrimarySubTypes() {
            return new HashSet<>();
        }

        @Override
        public Set<String> getMixinSubTypes() {
            return new HashSet<>();
        }

        @Override
        public boolean isMixin() {
            return false;
        }

        @Override
        public Iterable<String> getNamesSingleValuesProperties() {
            return new HashSet<>();
        }
    }
}
