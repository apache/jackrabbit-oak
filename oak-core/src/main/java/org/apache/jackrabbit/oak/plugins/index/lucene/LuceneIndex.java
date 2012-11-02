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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.PATH_SELECTOR;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.query.Query.JCR_PATH;

/**
 * Provides a QueryIndex that does lookups against a Lucene-based index
 * 
 * <p>
 * To define a lucene index on a subtree you have to add an <code>oak:index<code> node.
 * 
 * Under it follows the index definition node that:
 * <ul>
 * <li>must be of type <code>oak:queryIndexDefinition</code></li>
 * <li>must have the <code>type</code> property set to <b><code>lucene</code></b></li>
 * </ul>
 * </p>
 * 
 * <p>
 * Note: <code>reindex<code> is a property that when set to <code>true</code>, triggers a full content reindex.
 * </p>
 * 
 * <pre>
 * <code>
 * {
 *     NodeBuilder index = root.child("oak:index");
 *     index.child("lucene")
 *         .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
 *         .setProperty("type", "lucene")
 *         .setProperty("reindex", "true");
 * }
 * </code>
 * </pre>
 * 
 * @see QueryIndex
 * 
 */
public class LuceneIndex implements QueryIndex, LuceneIndexConstants {

    private static final Logger LOG = LoggerFactory
            .getLogger(LuceneIndex.class);

    private final IndexDefinition index;

    public LuceneIndex(IndexDefinition indexDefinition) {
        this.index = indexDefinition;
    }

    @Override
    public String getIndexName() {
        return index.getName();
    }

    @Override
    public double getCost(Filter filter) {
        return 1.0;
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        return getQuery(filter, root, null).toString();
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {

        NodeBuilder builder = new ReadOnlyBuilder(root);
        for (String name : elements(index.getPath())) {
            builder = builder.child(name);
        }
        if (!builder.hasChildNode(INDEX_DATA_CHILD_NAME)) {
            // index not initialized yet
            return new PathCursor(Collections.<String> emptySet());
        }
        builder = builder.child(INDEX_DATA_CHILD_NAME);

        Directory directory = new ReadOnlyOakDirectory(builder);
        long s = System.currentTimeMillis();

        try {
            try {
                IndexReader reader = DirectoryReader.open(directory);
                try {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Collection<String> paths = new ArrayList<String>();

                    Query query = getQuery(filter, root, reader);
                    if (query != null) {
                        TopDocs docs = searcher
                                .search(query, Integer.MAX_VALUE);
                        for (ScoreDoc doc : docs.scoreDocs) {
                            String path = reader.document(doc.doc,
                                    PATH_SELECTOR).get(PATH);
                            if ("".equals(path)) {
                                paths.add("/");
                            } else if (path != null) {
                                paths.add(path);
                            }
                        }
                    }
                    LOG.debug("query via {} took {} ms.", this,
                            System.currentTimeMillis() - s);
                    return new PathCursor(paths);
                } finally {
                    reader.close();
                }
            } finally {
                directory.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new PathCursor(Collections.<String> emptySet());
        }
    }

    private static Query getQuery(Filter filter, NodeState root, IndexReader reader) {
        List<Query> qs = new ArrayList<Query>();

        try {
            addNodeTypeConstraints(qs, filter.getNodeType(), root);
        } catch (RepositoryException e) {
            throw new RuntimeException(
                    "Unable to process node type constraints", e);
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
            // FIXME
            if (!path.endsWith("/")) {
                path += "/";
            }
            qs.add(new PrefixQuery(newPathTerm(path)));
            break;
        case EXACT:
            qs.add(new TermQuery(newPathTerm(path)));
            break;
        case PARENT:
            if (PathUtils.denotesRoot(path)) {
                // there's no parent of the root node
                return null;
            }
            qs.add(new TermQuery(newPathTerm(PathUtils.getParentPath(path))));
            break;
        }

        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String name = pr.propertyName;
            if (name.contains("/")) {
                // lucene cannot handle child-level property restrictions
                continue;
            }

            String first = null;
            String last = null;
            boolean isLike = pr.isLike;

            // TODO what to do with escaped tokens?
            if (pr.first != null) {
                first = pr.first.getValue(Type.STRING);
                first = first.replace("\\", "");
            }
            if (pr.last != null) {
                last = pr.last.getValue(Type.STRING);
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
                        qs.add(new TermQuery(new Term(name, first)));
                    }
                }
                continue;
            }

            qs.add(TermRangeQuery.newStringRange(name, first, last,
                    pr.firstIncluding, pr.lastIncluding));

        }

        if (qs.size() == 0) {
            return new MatchAllDocsQuery();
        }
        if (qs.size() == 1) {
            return qs.get(0);
        }
        BooleanQuery bq = new BooleanQuery();
        for (Query q : qs) {
            bq.add(q, Occur.MUST);
        }
        return bq;
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
            bq.add(new TermQuery(new Term(f, uuid)), Occur.SHOULD);
        }
        qs.add(bq);
    }

    private static void addNodeTypeConstraints(
            List<Query> qs, String name, NodeState root)
            throws RepositoryException {
        // TODO remove empty name check once OAK-359 is done
        if (NodeTypeConstants.NT_BASE.equals(name) || "".equals(name)) {
            return; // shortcut
        }
        NodeState system = root.getChildNode(NodeTypeConstants.JCR_SYSTEM);
        if (system == null) {
            return;
        }
        final NodeState types =
                system.getChildNode(NodeTypeConstants.JCR_NODE_TYPES);
        if (types == null) {
            return;
        }

        NodeTypeManager manager = new ReadOnlyNodeTypeManager() {
            @Override @CheckForNull
            protected Tree getTypes() {
                return new ReadOnlyTree(types);
            }
        };

        BooleanQuery bq = new BooleanQuery();
        NodeType type = manager.getNodeType(name);
        bq.add(createNodeTypeQuery(type), Occur.SHOULD);
        NodeTypeIterator iterator = type.getSubtypes();
        while (iterator.hasNext()) {
            bq.add(createNodeTypeQuery(iterator.nextNodeType()), Occur.SHOULD);
        }
        qs.add(bq);
    }

    private static Query createNodeTypeQuery(NodeType type) {
        String name = NodeTypeConstants.JCR_PRIMARYTYPE;
        if (type.isMixin()) {
            name = NodeTypeConstants.JCR_MIXINTYPES;
        }
        return new TermQuery(new Term(name, type.getName()));
    }

    /**
     * A cursor over the resulting paths.
     */
    private static class PathCursor implements Cursor {

        private final Iterator<String> iterator;

        private String path;

        public PathCursor(Collection<String> paths) {
            this.iterator = paths.iterator();
        }

        @Override
        public boolean next() {
            if (iterator.hasNext()) {
                path = iterator.next();
                return true;
            } else {
                path = null;
                return false;
            }
        }

        @Override
        public IndexRow currentRow() {
            // TODO support jcr:score and possibly rep:exceprt
            return new IndexRowImpl(path);
        }

    }

    @Override
    public String toString() {
        return "LuceneIndex [index=" + index + "]";
    }

}
