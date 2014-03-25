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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getAncestorPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH_SELECTOR;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_OAK;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newFulltextTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.skipTokenization;
import static org.apache.jackrabbit.oak.query.QueryImpl.JCR_PATH;
import static org.apache.jackrabbit.oak.spi.query.Cursors.newPathCursor;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.MUST_NOT;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.MoreLikeThisHelper;
import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a QueryIndex that does lookups against a Lucene-based index
 *
 * <p>
 * To define a lucene index on a subtree you have to add an
 * <code>oak:index<code> node.
 *
 * Under it follows the index definition node that:
 * <ul>
 * <li>must be of type <code>oak:QueryIndexDefinition</code></li>
 * <li>must have the <code>type</code> property set to <b><code>lucene</code></b></li>
 * <li>must have the <code>async</code> property set to <b><code>async</code></b></li>
 * </b></li>
 * </ul>
 * </p>
 * <p>
 * Optionally you can add
 * <ul>
 * <li>what subset of property types to be included in the index via the <code>includePropertyTypes<code> property</li>
 * <li>a blacklist of property names: what property to be excluded from the index via the <code>excludePropertyNames<code> property</li>
 * <li>the <code>reindex<code> flag which when set to <code>true<code>, triggers a full content re-index.</li>
 * </ul>
 * </p>
 * <pre>
 * <code>
 * {
 *     NodeBuilder index = root.child("oak:index");
 *     index.child("lucene")
 *         .setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME)
 *         .setProperty("type", "lucene")
 *         .setProperty("async", "async")
 *         .setProperty("reindex", "true");
 * }
 * </code>
 * </pre>
 *
 * @see QueryIndex
 *
 */
public class LuceneIndex implements FulltextQueryIndex {

    private static final Logger LOG = LoggerFactory
            .getLogger(LuceneIndex.class);
    public static final String NATIVE_QUERY_FUNCTION = "native*lucene";

    private final Analyzer analyzer;

    private final NodeAggregator aggregator;

    public LuceneIndex(Analyzer analyzer, NodeAggregator aggregator) {
        this.analyzer = analyzer;
        this.aggregator = aggregator;
    }

    @Override
    public String getIndexName() {
        return "lucene";
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        if (!isLive(root)) {
            // unusable index
            return Double.POSITIVE_INFINITY;
        }
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            // no full-text condition: don't use this index,
            // as there might be a better one
            return Double.POSITIVE_INFINITY;
        }
        Set<String> relPaths = getRelativePaths(ft);
        if (relPaths.size() > 1) {
            LOG.warn("More than one relative parent for query " + filter.getQueryStatement());
            // there are multiple "parents", as in
            // "contains(a/x, 'hello') and contains(b/x, 'world')"
            return new MultiLuceneIndex(filter, root, relPaths).getCost();
        }
        String parent = relPaths.iterator().next();
        if (parent.isEmpty()) {
            // no relative properties
            return 10;
        }
        // all relative properties have the same "parent", as in
        // "contains(a/x, 'hello') and contains(a/y, 'world')" or
        // "contains(a/x, 'hello') or contains(a/*, 'world')"
        // TODO: proper cost calculation
        // we assume this will cause more read operations,
        // as we need to read the node and then the parent
        return 15;
    }

    /**
     * Get the set of relative paths of a full-text condition. For example, for
     * the condition "contains(a/b, 'hello') and contains(c/d, 'world'), the set
     * { "a", "c" } is returned. If there are no relative properties, then one
     * entry is returned (the empty string). If there is no expression, then an
     * empty set is returned.
     *
     * @param ft the full-text expression
     * @return the set of relative paths (possibly empty)
     */
    private static Set<String> getRelativePaths(FullTextExpression ft) {
        if (ft == null) {
            // there might be no full-text constraint when using the
            // LowCostLuceneIndexProvider which is used for testing
            // TODO if the LowCostLuceneIndexProvider is removed, we should do
            // the following instead:

            // throw new
            // IllegalStateException("Lucene index is used even when no full-text conditions are used for filter "
            // + filter);

            return Collections.emptySet();
        }
        final HashSet<String> relPaths = new HashSet<String>();
        ft.accept(new FullTextVisitor.FullTextVisitorBase() {

            @Override
            public boolean visit(FullTextTerm term) {
                String p = term.getPropertyName();
                if (p == null) {
                    relPaths.add("");
                } else if (p.startsWith("../") || p.startsWith("./")) {
                    throw new IllegalArgumentException("Relative parent is not supported:" + p);
                } else if (getDepth(p) > 1) {
                    String parent = getParentPath(p);
                    relPaths.add(parent);
                } else {
                    relPaths.add("");
                }
                return true;
            }
        });
        return relPaths;
    }

    private static boolean isLive(NodeState root) {
        NodeState def = getIndexDef(root);
        if (def == null) {
            return false;
        }
        String type = def.getString(PERSISTENCE_NAME);
        if (type == null || PERSISTENCE_OAK.equalsIgnoreCase(type)) {
            return getIndexDataNode(def) != null;
        }

        if (PERSISTENCE_FILE.equalsIgnoreCase(type)) {
            return def.getString(PERSISTENCE_PATH) != null;
        }

        return false;
    }

    private static Directory newDirectory(NodeState root) {
        NodeState def = getIndexDef(root);
        if (def == null) {
            return null;
        }

        String type = def.getString(PERSISTENCE_NAME);
        if (type == null || PERSISTENCE_OAK.equalsIgnoreCase(type)) {
            NodeState index = getIndexDataNode(def);
            if (index == null) {
                return null;
            }
            return new OakDirectory(new ReadOnlyBuilder(index));
        }

        if (PERSISTENCE_FILE.equalsIgnoreCase(type)) {
            String fs = def.getString(PERSISTENCE_PATH);
            if (fs == null) {
                return null;
            }
            File f = new File(fs);
            if (!f.exists()) {
                return null;
            }
            try {
                // TODO lock factory
                return FSDirectory.open(f);
            } catch (IOException e) {
                LOG.error("Unable to open directory {}", fs);
            }
        }

        return null;
    }

    private static NodeState getIndexDef(NodeState node) {
        NodeState state = node.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            NodeState ns = entry.getNodeState();
            if (TYPE_LUCENE.equals(ns.getString(TYPE_PROPERTY_NAME))) {
                return ns;
            }
        }
        return null;
    }

    private static NodeState getIndexDataNode(NodeState node) {
        if (node.hasChildNode(INDEX_DATA_CHILD_NAME)) {
            return node.getChildNode(INDEX_DATA_CHILD_NAME);
        }
        // unusable index (not initialized yet)
        return null;
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        FullTextExpression ft = filter.getFullTextConstraint();
        Set<String> relPaths = getRelativePaths(ft);
        if (relPaths.size() > 1) {
            return new MultiLuceneIndex(filter, root, relPaths).getPlan();
        }
        String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();
        // we only restrict non-full-text conditions if there is
        // no relative property in the full-text constraint
        boolean nonFullTextConstraints = parent.isEmpty();
        String plan = getQuery(filter, null, nonFullTextConstraints, analyzer) + " ft:(" + ft + ")";
        if (!parent.isEmpty()) {
            plan += " parent:" + parent;
        }
        return plan;
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        if (!isLive(root)) {
            throw new IllegalStateException("Lucene index is not live");
        }
        FullTextExpression ft = filter.getFullTextConstraint();
        Set<String> relPaths = getRelativePaths(ft);
        if (relPaths.size() > 1) {
            return new MultiLuceneIndex(filter, root, relPaths).query();
        }
        String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();
        // we only restrict non-full-text conditions if there is
        // no relative property in the full-text constraint
        boolean nonFullTextConstraints = parent.isEmpty();
        Directory directory = newDirectory(root);
        if (directory == null) {
            return newPathCursor(Collections.<String> emptySet());
        }
        long s = System.currentTimeMillis();
        try {
            try {
                IndexReader reader = DirectoryReader.open(directory);
                try {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Collection<String> paths = new ArrayList<String>();
                    Query query = getQuery(filter, reader,
                            nonFullTextConstraints, analyzer);

                    // TODO OAK-828
                    HashSet<String> seenPaths = new HashSet<String>();
                    int parentDepth = getDepth(parent);
                    if (query != null) {
                        // OAK-925
                        // TODO how to best avoid loading all entries in memory?
                        // (memory problem and performance problem)
                        TopDocs docs = searcher
                                .search(query, Integer.MAX_VALUE);
                        for (ScoreDoc doc : docs.scoreDocs) {
                            String path = reader.document(doc.doc,
                                    PATH_SELECTOR).get(PATH);
                            if (path != null) {
                                if ("".equals(path)) {
                                    path = "/";
                                }
                                if (!parent.isEmpty()) {
                                    // TODO OAK-828 this breaks node aggregation
                                    // get the base path
                                    // ensure the path ends with the given
                                    // relative path
                                    // if (!path.endsWith("/" + parent)) {
                                    // continue;
                                    // }
                                    path = getAncestorPath(path, parentDepth);
                                    // avoid duplicate entries
                                    if (seenPaths.contains(path)) {
                                        continue;
                                    }
                                    seenPaths.add(path);
                                }

                                paths.add(path);
                            }
                        }
                    }
                    LOG.debug("query via {} took {} ms.", this,
                            System.currentTimeMillis() - s);
                    return newPathCursor(paths);
                } finally {
                    reader.close();
                }
            } finally {
                directory.close();
            }
        } catch (IOException e) {
            LOG.warn("query via {} failed.", this, e);
            return newPathCursor(Collections.<String> emptySet());
        }
    }

    /**
     * Get the Lucene query for the given filter.
     *
     * @param filter the filter, including full-text constraint
     * @param reader the Lucene reader
     * @param nonFullTextConstraints whether non-full-text constraints (such a
     *            path, node type, and so on) should be added to the Lucene
     *            query
     * @param analyzer the Lucene analyzer used for building the fulltext query
     * @return the Lucene query
     */
    private static Query getQuery(Filter filter, IndexReader reader,
            boolean nonFullTextConstraints, Analyzer analyzer) {
        List<Query> qs = new ArrayList<Query>();
        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft == null) {
            // there might be no full-text constraint
            // when using the LowCostLuceneIndexProvider
            // which is used for testing
        } else {
            qs.add(getFullTextQuery(ft, analyzer, reader));
        }
        PropertyRestriction pr = filter.getPropertyRestriction(NATIVE_QUERY_FUNCTION);
        if (pr != null) {
            String query = String.valueOf(pr.first.getValue(pr.first.getType()));
            QueryParser queryParser = new QueryParser(VERSION, "", analyzer);
            if (query.startsWith("mlt?")) {
                String mltQueryString = query.replace("mlt?", "");
                if (reader != null) {
                    Query moreLikeThis = MoreLikeThisHelper.getMoreLikeThis(reader, analyzer, mltQueryString);
                    if (moreLikeThis != null) {
                        qs.add(moreLikeThis);
                    }
                }
            }
            else {
                try {
                    qs.add(queryParser.parse(query));
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        else if (nonFullTextConstraints) {
            addNonFullTextConstraints(qs, filter, reader, analyzer);
        }
        if (qs.size() == 0) {
            return new MatchAllDocsQuery();
        }
        if (qs.size() == 1) {
            return qs.get(0);
        }
        BooleanQuery bq = new BooleanQuery();
        for (Query q : qs) {
            bq.add(q, MUST);
        }
        return bq;
    }

    private static void addNonFullTextConstraints(List<Query> qs,
            Filter filter, IndexReader reader, Analyzer analyzer) {
        if (!filter.matchesAllTypes()) {
            addNodeTypeConstraints(qs, filter);
        }

        String path = filter.getPath();
        switch (filter.getPathRestriction()) {
        case ALL_CHILDREN:
            if ("/".equals(path)) {
                break;
            }
            if (!path.endsWith("/")) {
                path += "/";
            }
            qs.add(new PrefixQuery(newPathTerm(path)));
            break;
        case DIRECT_CHILDREN:
            if (!path.endsWith("/")) {
                path += "/";
            }
            qs.add(new PrefixQuery(newPathTerm(path)));
            break;
        case EXACT:
            qs.add(new TermQuery(newPathTerm(path)));
            break;
        case PARENT:
            if (denotesRoot(path)) {
                // there's no parent of the root node
                // we add a path that can not possibly occur because there
                // is no way to say "match no documents" in Lucene
                qs.add(new TermQuery(new Term(FieldNames.PATH, "///")));
            } else {
                qs.add(new TermQuery(newPathTerm(getParentPath(path))));
            }
            break;
        case NO_RESTRICTION:
            break;
        }

        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {

            if (pr.first == null && pr.last == null) {
                // ignore property existence checks, Lucene can't to 'property
                // is not null' queries (OAK-1208)
                continue;
            }

            String name = pr.propertyName;
            if (name.contains("/")) {
                // lucene cannot handle child-level property restrictions
                continue;
            }
            if ("rep:excerpt".equals(name)) {
                continue;
            }
            if (JCR_PRIMARYTYPE.equals(name)) {
                continue;
            }

            if (skipTokenization(name)) {
                qs.add(new TermQuery(new Term(name, pr.first
                        .getValue(STRING))));
                continue;
            }

            String first = null;
            String last = null;
            boolean isLike = pr.isLike;

            // TODO what to do with escaped tokens?
            if (pr.first != null) {
                first = pr.first.getValue(STRING);
                first = first.replace("\\", "");
            }
            if (pr.last != null) {
                last = pr.last.getValue(STRING);
                last = last.replace("\\", "");
            }

            if (isLike) {
                first = first.replace('%', WildcardQuery.WILDCARD_STRING);
                first = first.replace('_', WildcardQuery.WILDCARD_CHAR);

                int indexOfWS = first.indexOf(WildcardQuery.WILDCARD_STRING);
                int indexOfWC = first.indexOf(WildcardQuery.WILDCARD_CHAR);
                int len = first.length();

                if (indexOfWS == len || indexOfWC == len) {
                    // remove trailing "*" for prefixquery
                    first = first.substring(0, first.length() - 1);
                    if (JCR_PATH.equals(name)) {
                        qs.add(new PrefixQuery(newPathTerm(first)));
                    } else {
                        qs.add(new PrefixQuery(new Term(name, first)));
                    }
                } else {
                    if (JCR_PATH.equals(name)) {
                        qs.add(new WildcardQuery(newPathTerm(first)));
                    } else {
                        qs.add(new WildcardQuery(new Term(name, first)));
                    }
                }
                continue;
            }

            if (first != null && first.equals(last) && pr.firstIncluding
                    && pr.lastIncluding) {
                if (JCR_PATH.equals(name)) {
                    qs.add(new TermQuery(newPathTerm(first)));
                } else {
                    if ("*".equals(name)) {
                        addReferenceConstraint(first, qs, reader);
                    } else {
                        for (String t : tokenize(first, analyzer)) {
                            qs.add(new TermQuery(new Term(name, t)));
                        }
                    }
                }
                continue;
            }

            first = tokenizeAndPoll(first, analyzer);
            last = tokenizeAndPoll(last, analyzer);
            qs.add(TermRangeQuery.newStringRange(name, first, last,
                    pr.firstIncluding, pr.lastIncluding));
        }
    }

    private static String tokenizeAndPoll(String token, Analyzer analyzer){
        if (token != null) {
            List<String> tokens = tokenize(token, analyzer);
            if (!tokens.isEmpty()) {
                token = tokens.get(0);
            }
        }
        return token;
    }

    private static void addReferenceConstraint(String uuid, List<Query> qs,
            IndexReader reader) {
        if (reader == null) {
            // getPlan call
            qs.add(new TermQuery(new Term("*", uuid)));
            return;
        }

        // reference query
        BooleanQuery bq = new BooleanQuery();
        Collection<String> fields = MultiFields.getIndexedFields(reader);
        for (String f : fields) {
            bq.add(new TermQuery(new Term(f, uuid)), SHOULD);
        }
        qs.add(bq);
    }

    private static void addNodeTypeConstraints(List<Query> qs, Filter filter) {
        BooleanQuery bq = new BooleanQuery();
        for (String type : filter.getPrimaryTypes()) {
            bq.add(new TermQuery(new Term(JCR_PRIMARYTYPE, type)), SHOULD);
        }
        for (String type : filter.getMixinTypes()) {
            bq.add(new TermQuery(new Term(JCR_MIXINTYPES, type)), SHOULD);
        }
        qs.add(bq);
    }

    static Query getFullTextQuery(FullTextExpression ft, final Analyzer analyzer, final IndexReader reader) {
        // a reference to the query, so it can be set in the visitor
        // (a "non-local return")
        final AtomicReference<Query> result = new AtomicReference<Query>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextOr or) {
                BooleanQuery q = new BooleanQuery();
                for (FullTextExpression e : or.list) {
                    Query x = getFullTextQuery(e, analyzer, reader);
                    q.add(x, SHOULD);
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BooleanQuery q = new BooleanQuery();
                for (FullTextExpression e : and.list) {
                    Query x = getFullTextQuery(e, analyzer, reader);
                    // Lucene can't deal with "must(must_not(x))"
                    if (x instanceof BooleanQuery) {
                        BooleanQuery bq = (BooleanQuery) x;
                        for (BooleanClause c : bq.clauses()) {
                            q.add(c);
                        }
                    } else {
                        q.add(x, MUST);
                    }
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                String p = term.getPropertyName();
                if (p != null && p.indexOf('/') >= 0) {
                    p = getName(p);
                }
                Query q = tokenToQuery(term.getText(), p, analyzer, reader);
                if (q == null) {
                    return false;
                }
                String boost = term.getBoost();
                if (boost != null) {
                    q.setBoost(Float.parseFloat(boost));
                }
                if (term.isNot()) {
                    BooleanQuery bq = new BooleanQuery();
                    bq.add(q, MUST_NOT);
                    result.set(bq);
                } else {
                    result.set(q);
                }
                return true;
            }
        });
        return result.get();
    }

    static Query tokenToQuery(String text, String fieldName, Analyzer analyzer, IndexReader reader) {
        if (analyzer == null) {
            return null;
        }
        List<String> tokens = tokenize(text, analyzer);

        if (tokens.isEmpty()) {
            // TODO what should be returned in the case there are no tokens?
            return new BooleanQuery();
        }
        if (tokens.size() == 1) {
            String token = tokens.iterator().next();
            if (hasFulltextToken(token)) {
                return new WildcardQuery(newFulltextTerm(token, fieldName));
            } else {
                return new TermQuery(newFulltextTerm(token, fieldName));
            }
        } else {
            if (hasFulltextToken(tokens)) {
                MultiPhraseQuery mpq = new MultiPhraseQuery();
                for(String token: tokens){
                    if (hasFulltextToken(token)) {
                        Term[] terms = extractMatchingTokens(reader, fieldName, token);
                        if (terms != null && terms.length > 0) {
                            mpq.add(terms);
                        }
                    } else {
                        mpq.add(newFulltextTerm(token, fieldName));
                    }
                }
                return mpq;
            } else {
                PhraseQuery pq = new PhraseQuery();
                for (String t : tokens) {
                    pq.add(newFulltextTerm(t, fieldName));
                }
                return pq;
            }
        }
    }

    private static Term[] extractMatchingTokens(IndexReader reader, String fieldName, String token) {
        if (reader == null) {
            // getPlan call
            return null;
        }

        try {
            List<Term> terms = new ArrayList<Term>();
            Term onTerm = newFulltextTerm(token, fieldName);
            Terms t = MultiFields.getTerms(reader, onTerm.field());
            Automaton a = WildcardQuery.toAutomaton(onTerm);
            CompiledAutomaton ca = new CompiledAutomaton(a);
            TermsEnum te = ca.getTermsEnum(t);
            BytesRef text;
            while ((text = te.next()) != null) {
                terms.add(newFulltextTerm(text.utf8ToString(), fieldName));
            }
            return terms.toArray(new Term[terms.size()]);
        } catch (IOException e) {
            LOG.error("Building fulltext query failed", e.getMessage());
            return null;
        }
    }

    private static boolean hasFulltextToken(List<String> tokens) {
        for (String token : tokens) {
            if (hasFulltextToken(token)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasFulltextToken(String token) {
        for (char c : fulltextTokens) {
            if (token.indexOf(c) != -1) {
                return true;
            }
        }
        return false;
    }

    private static char[] fulltextTokens = new char[] { '*', '?' };

    /**
     * Tries to merge back tokens that are split on relevant fulltext query
     * wildcards ('*' or '?')
     *
     *
     * @param text
     * @param analyzer
     * @return
     */
    static List<String> tokenize(String text, Analyzer analyzer) {
        List<String> tokens = new ArrayList<String>();
        TokenStream stream = null;
        try {
            stream = analyzer.tokenStream(FieldNames.FULLTEXT,
                    new StringReader(text));
            CharTermAttribute termAtt = stream
                    .addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = stream
                    .addAttribute(OffsetAttribute.class);
            // TypeAttribute type = stream.addAttribute(TypeAttribute.class);

            stream.reset();

            int poz = 0;
            boolean hasFulltextToken = false;
            StringBuilder token = new StringBuilder();
            while (stream.incrementToken()) {
                String term = termAtt.toString();
                int start = offsetAtt.startOffset();
                int end = offsetAtt.endOffset();
                if (start > poz) {
                    for (int i = poz; i < start; i++) {
                        for (char c : fulltextTokens) {
                            if (c == text.charAt(i)) {
                                token.append(c);
                                hasFulltextToken = true;
                            }
                        }
                    }
                }
                poz = end;
                if (hasFulltextToken) {
                    token.append(term);
                    hasFulltextToken = false;
                } else {
                    if (token.length() > 0) {
                        tokens.add(token.toString());
                    }
                    token = new StringBuilder();
                    token.append(term);
                }
            }
            // consume to the end of the string
            if (poz < text.length()) {
                for (int i = poz; i < text.length(); i++) {
                    for (char c : fulltextTokens) {
                        if (c == text.charAt(i)) {
                            token.append(c);
                        }
                    }
                }
            }
            if (token.length() > 0) {
                tokens.add(token.toString());
            }
            stream.end();
        } catch (IOException e) {
            LOG.error("Building fulltext query failed", e.getMessage());
            return null;
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return tokens;
    }

    @Override
    public NodeAggregator getNodeAggregator() {
        return aggregator;
    }

}
