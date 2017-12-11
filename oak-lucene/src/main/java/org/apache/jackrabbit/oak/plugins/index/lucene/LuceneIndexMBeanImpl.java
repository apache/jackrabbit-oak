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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeTraverser;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.Name;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.lucene.BadIndexTracker.BadIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.PropertyIndexCleaner;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.PathStoredFieldVisitor;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker.Level;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker.Result;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newAncestorTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryUtils.dirSize;

public class LuceneIndexMBeanImpl extends AnnotatedStandardMBean implements LuceneIndexMBean {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IndexTracker indexTracker;
    private final NodeStore nodeStore;
    private final IndexPathService indexPathService;
    private final File workDir;
    private final PropertyIndexCleaner propertyIndexCleaner;

    public LuceneIndexMBeanImpl(IndexTracker indexTracker, NodeStore nodeStore, IndexPathService indexPathService, File workDir, @Nullable PropertyIndexCleaner cleaner) {
        super(LuceneIndexMBean.class);
        this.indexTracker = checkNotNull(indexTracker);
        this.nodeStore = checkNotNull(nodeStore);
        this.indexPathService = indexPathService;
        this.workDir = checkNotNull(workDir);
        this.propertyIndexCleaner = cleaner;
    }

    @Override
    public TabularData getIndexStats() throws IOException {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(LuceneIndexMBeanImpl.class.getName(),
                    "Lucene Index Stats", IndexStats.TYPE, new String[]{"path"});
            tds = new TabularDataSupport(tt);
            Set<String> indexes = indexTracker.getIndexNodePaths();
            for (String path : indexes) {
                IndexNode indexNode = null;
                try {
                    indexNode = indexTracker.acquireIndexNode(path);
                    if (indexNode != null) {
                        IndexStats stats = new IndexStats(path, indexNode);
                        tds.put(stats.toCompositeData());
                    }
                } finally {
                    if (indexNode != null) {
                        indexNode.release();
                    }
                }
            }
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }

    @Override
    public TabularData getBadIndexStats() {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(LuceneIndexMBeanImpl.class.getName(),
                    "Lucene Bad Index Stats", BadIndexStats.TYPE, new String[]{"path"});
            tds = new TabularDataSupport(tt);
            Set<String> indexes = indexTracker.getBadIndexTracker().getIndexPaths();
            for (String path : indexes) {
                BadIndexInfo info = indexTracker.getBadIndexTracker().getInfo(path);
                if (info != null){
                    BadIndexStats stats = new BadIndexStats(info);
                    tds.put(stats.toCompositeData());
                }
            }
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }

    @Override
    public TabularData getBadPersistedIndexStats() {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(LuceneIndexMBeanImpl.class.getName(),
                    "Lucene Bad Persisted Index Stats", BadIndexStats.TYPE, new String[]{"path"});
            tds = new TabularDataSupport(tt);
            Set<String> indexes = indexTracker.getBadIndexTracker().getBadPersistedIndexPaths();
            for (String path : indexes) {
                BadIndexInfo info = indexTracker.getBadIndexTracker().getPersistedIndexInfo(path);
                if (info != null){
                    BadIndexStats stats = new BadIndexStats(info);
                    tds.put(stats.toCompositeData());
                }
            }
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }

    @Override
    public boolean isFailing() {
        return indexTracker.getBadIndexTracker().hasBadIndexes();
    }

    @Override
    public String[] getIndexedPaths(String indexPath, int maxLevel, int maxPathCount) throws IOException {
        IndexNode indexNode = null;
        try {
            if(indexPath == null){
                indexPath = "/";
            }

            indexNode = indexTracker.acquireIndexNode(indexPath);
            if (indexNode != null) {
                IndexDefinition defn = indexNode.getDefinition();
                if (!defn.evaluatePathRestrictions()){
                    String msg = String.format("Index at [%s] does not have [%s] enabled. So paths statistics cannot " +
                            "be determined for this index", indexPath, LuceneIndexConstants.EVALUATE_PATH_RESTRICTION);
                    return createMsg(msg);
                }

                IndexSearcher searcher = indexNode.getSearcher();
                return determineIndexedPaths(searcher, maxLevel, maxPathCount);
            }
        } finally {
            if (indexNode != null) {
                indexNode.release();
            }
        }
        return new String[0];
    }

    @Override
    public String[] getFieldInfo(String indexPath) throws IOException {
        TreeSet<String> indexes = new TreeSet<String>();
        if (indexPath == null || indexPath.isEmpty()) {
            indexes.addAll(indexTracker.getIndexNodePaths());
        } else {
            indexes.add(indexPath);
        }
        ArrayList<String> list = new ArrayList<String>();
        for (String path : indexes) {
            IndexNode indexNode = null;
            try {
                indexNode = indexTracker.acquireIndexNode(path);
                if (indexNode != null) {
                    IndexSearcher searcher = indexNode.getSearcher();
                    list.addAll(getFieldInfo(path, searcher));
                }
            } finally {
                if (indexNode != null) {
                    indexNode.release();
                }
            }
        }
        return list.toArray(new String[0]);
    }

    @Override
    public String[] getFieldTermsInfo(String indexPath, String field, int max) throws IOException {
        return getFieldTermPrefixInfo(indexPath, field, max, null);
    }

    @Override
    public String[] getFieldTermInfo(String indexPath, String field, String term) throws IOException {
        return getFieldTermPrefixInfo(indexPath, field, Integer.MAX_VALUE, term);
    }

    private String[] getFieldTermPrefixInfo(String indexPath, String field, int max, String term) throws IOException {
        TreeSet<String> indexes = new TreeSet<String>();
        if (indexPath == null || indexPath.isEmpty()) {
            indexes.addAll(indexTracker.getIndexNodePaths());
        } else {
            indexes.add(indexPath);
        }
        ArrayList<String> list = new ArrayList<String>();
        for (String path : indexes) {
            IndexNode indexNode = null;
            try {
                indexNode = indexTracker.acquireIndexNode(path);
                if (indexNode != null) {
                    IndexSearcher searcher = indexNode.getSearcher();
                    list.addAll(getFieldTerms(path, field, max, term, searcher));
                }
            } finally {
                if (indexNode != null) {
                    indexNode.release();
                }
            }
        }
        return list.toArray(new String[0]);
    }

    @Override
    public String getStoredIndexDefinition(@Name("indexPath") String indexPath) {
        IndexDefinition defn = indexTracker.getIndexDefinition(indexPath);
        NodeState state;
        if (defn != null){
            state = defn.getDefinitionNodeState();
        } else {
            state = NodeStateUtils.getNode(indexTracker.getRoot(), indexPath + "/" + INDEX_DEFINITION_NODE);
        }

        if (state.exists()){
            return NodeStateUtils.toString(state);
        }
        return "No index found at given path";
    }

    @Override
    public String diffStoredIndexDefinition(@Name("indexPath") String indexPath) {
        NodeState stored = NodeStateUtils.getNode(indexTracker.getRoot(), indexPath + "/" + INDEX_DEFINITION_NODE);
        NodeState current = NodeStateUtils.getNode(indexTracker.getRoot(), indexPath);
        if (stored.exists()){
            current = NodeStateCloner.cloneVisibleState(current);
            JsopDiff diff = new JsopDiff();
            current.compareAgainstBaseState(stored, diff);
            return JsopBuilder.prettyPrint(diff.toString());
        }
        return "No stored index definition found at given path";
    }

    @Override
    public String checkConsistency(String indexPath, boolean fullCheck) throws IOException {
        NodeState indexState = NodeStateUtils.getNode(nodeStore.getRoot(), indexPath);
        checkArgument(indexState.exists(), "No node exist at path [%s]", indexPath);
        return getConsistencyCheckResult(indexPath, fullCheck).toString();
    }

    @Override
    public String[] checkAndReportConsistencyOfAllIndexes(boolean fullCheck) throws IOException {
        Stopwatch watch = Stopwatch.createStarted();
        List<String> results = new ArrayList<>();
        NodeState root = nodeStore.getRoot();
        for (String indexPath : indexPathService.getIndexPaths()) {
            NodeState idxState = NodeStateUtils.getNode(root, indexPath);
            if (LuceneIndexConstants.TYPE_LUCENE.equals(idxState.getString(IndexConstants.TYPE_PROPERTY_NAME))) {
                Result result = getConsistencyCheckResult(indexPath, fullCheck);
                String msg = "OK";
                if (!result.clean) {
                    msg = "NOT OK";
                }
                results.add(String.format("%s : %s", indexPath, msg));
            }
        }
        log.info("Checked index consistency in {}. Check result {}", watch, results);
        return Iterables.toArray(results, String.class);
    }

    @Override
    public boolean checkConsistencyOfAllIndexes(boolean fullCheck) throws IOException {
        Stopwatch watch = Stopwatch.createStarted();
        NodeState root = nodeStore.getRoot();
        boolean clean = true;
        for (String indexPath : indexPathService.getIndexPaths()) {
            NodeState idxState = NodeStateUtils.getNode(root, indexPath);
            if (LuceneIndexConstants.TYPE_LUCENE.equals(idxState.getString(IndexConstants.TYPE_PROPERTY_NAME))) {
                Result result = getConsistencyCheckResult(indexPath, fullCheck);
                if (!result.clean) {
                    clean = false;
                    break;
                }
            }
        }
        log.info("Checked index consistency in {}. Check result {}", watch, clean);
        return clean;
    }

    @Override
    public String performPropertyIndexCleanup() throws CommitFailedException {
        String result = "PropertyIndexCleaner not enabled";
        if (propertyIndexCleaner != null) {
            result = propertyIndexCleaner.performCleanup(true).toString();
        }
        log.info("Explicit cleanup run done with result {}", result);
        return result;
    }

    @Override
    public String getHybridIndexInfo(String indexPath) {
        NodeState idx = NodeStateUtils.getNode(nodeStore.getRoot(), indexPath);
        return new HybridPropertyIndexInfo(idx).getInfoAsJson();
    }

    private Result getConsistencyCheckResult(String indexPath, boolean fullCheck) throws IOException {
        NodeState root = nodeStore.getRoot();
        Level level = fullCheck ? Level.FULL : Level.BLOBS_ONLY;
        IndexConsistencyChecker checker = new IndexConsistencyChecker(root, indexPath, workDir);
        return checker.check(level);
    }

    public void dumpIndexContent(String sourcePath, String destPath) throws IOException {
        IndexNode indexNode = null;
        try {
            if(sourcePath == null){
                sourcePath = "/";
            }

            indexNode = indexTracker.acquireIndexNode(sourcePath);
            if (indexNode != null) {
                log.info("Dumping Lucene directory content for [{}] to [{}]", sourcePath, destPath);
                Directory source = getDirectory(getPrimaryReader(indexNode.getPrimaryReaders()));
                checkNotNull(source, "IndexSearcher not backed by DirectoryReader");
                Directory dest = FSDirectory.open(new File(destPath));
                for (String file : source.listAll()) {
                    source.copy(dest, file, file, IOContext.DEFAULT);
                }
            }
        } finally {
            if (indexNode != null) {
                indexNode.release();
            }
        }
    }

    private static ArrayList<String> getFieldInfo(String path, IndexSearcher searcher) throws IOException {
        ArrayList<String> list = new ArrayList<String>();
        IndexReader reader = searcher.getIndexReader();
        Fields fields = MultiFields.getFields(reader);
        if (fields != null) {
            for(String f : fields) {
                list.add(path + " " + f + " " + reader.getDocCount(f));
            }
        }
        return list;
    }

    private static ArrayList<String> getFieldTerms(String path,
            String field, int max, String term, IndexSearcher searcher) throws IOException {
        if (field == null || field.isEmpty()) {
            ArrayList<String> list = new ArrayList<String>();
            IndexReader reader = searcher.getIndexReader();
            Fields fields = MultiFields.getFields(reader);
            if (fields != null) {
                for(String f : fields) {
                    list.addAll(getFieldTerms(path, f, max, term, searcher));
                }
            }
            return list;
        }
        IndexReader reader = searcher.getIndexReader();
        Terms terms = MultiFields.getTerms(reader, field);
        ArrayList<String> result = new ArrayList<>();
        if (terms == null) {
            return result;
        }
        TermsEnum iterator = terms.iterator(null);
        BytesRef byteRef = null;
        class Entry implements Comparable<Entry> {
            String term;
            int count;
            @Override
            public int compareTo(Entry o) {
                int c = Integer.compare(count, o.count);
                if (c == 0) {
                    c = term.compareTo(o.term);
                }
                return -c;
            }
        }
        ArrayList<Entry> list = new ArrayList<>();
        long totalCount = 0;
        while((byteRef = iterator.next()) != null) {
            Entry e = new Entry();
            e.term = byteRef.utf8ToString();
            if (term != null) {
                if (e.term != null && !e.term.equals(term)) {
                    continue;
                }
            }
            e.count = iterator.docFreq();
            totalCount += e.count;
            if (e.count > 1) {
                list.add(e);
            }
            if (max > 0 && list.size() > 2 * max) {
                sortAndTruncateList(list, max);
            }
        }
        sortAndTruncateList(list, max);
        result.add(totalCount + " (total for field " + field + ")");
        for(Entry e : list) {
            result.add(e.count + " " + e.term);
        }
        return result;
    }

    static <T extends Comparable<T>> void sortAndTruncateList(ArrayList<T> list, int max) {
        Collections.sort(list);
        if (max > 0 && list.size() > max) {
            list.subList(max, list.size()).clear();
        }
    }

    private static String[] determineIndexedPaths(IndexSearcher searcher, final int maxLevel, int maxPathCount)
            throws IOException {
        Set<String> paths = Sets.newHashSet();
        int startDepth = getStartDepth(searcher, maxLevel);
        if (startDepth < 0){
            return createMsg("startDepth cannot be determined after search for upto maxLevel ["+maxLevel+"]");
        }

        SearchContext sc = new SearchContext(searcher);
        List<LuceneDoc> docs = getDocsAtLevel(startDepth, sc);
        int maxPathLimitBreachedAtLevel = -1;
        topLevel:
        for (LuceneDoc doc : docs){
            TreeTraverser<LuceneDoc> traverser = new TreeTraverser<LuceneDoc>() {
                @Override
                public Iterable<LuceneDoc> children(@Nonnull LuceneDoc root) {
                    //Break at maxLevel
                    if (root.depth >= maxLevel) {
                        return Collections.emptyList();
                    }
                    return root.getChildren();
                }
            };

            for (LuceneDoc node : traverser.breadthFirstTraversal(doc)) {
                if (paths.size() < maxPathCount) {
                    paths.add(node.path);
                } else {
                    maxPathLimitBreachedAtLevel = node.depth;
                    break topLevel;
                }
            }
        }
        if (maxPathLimitBreachedAtLevel < 0) {
            return Iterables.toArray(paths, String.class);
        }

        //If max limit for path is reached then we can safely
        //say about includedPaths upto depth = level at which limit reached - 1
        //As for that level we know *all* the path roots
        Set<String> result = Sets.newHashSet();
        int safeDepth = maxPathLimitBreachedAtLevel - 1;
        if (safeDepth > 0) {
            for (String path : paths) {
                int pathDepth = PathUtils.getDepth(path);
                if (pathDepth == safeDepth) {
                    result.add(path);
                }
            }
        }
        return Iterables.toArray(result, String.class);
    }

    /**
     * Look for the startDepth. An index might have dat only at paths like /a/b/c so
     * to determine the start depth which needs to be used for query we need to find
     * out depth at which we start getting any entry
     */
    private static int getStartDepth(IndexSearcher searcher, int maxLevel) throws IOException {
        int depth = 0;
        while(depth < maxLevel){
            //Confirm if we have any hit at current depth
            TopDocs docs = searcher.search(newDepthQuery(depth), 1);
            if (docs.totalHits != 0){
                return depth;
            }
            depth++;
        }
        return -1;
    }

    private static List<LuceneDoc> getDocsAtLevel(int startDepth, SearchContext sc) throws IOException {
        TopDocs docs = sc.searcher.search(newDepthQuery(startDepth), Integer.MAX_VALUE);
        return getLuceneDocs(docs, sc);
    }

    private static class SearchContext{
        final IndexSearcher searcher;

        SearchContext(IndexSearcher searcher) {
            this.searcher = searcher;
        }
    }

    private static class LuceneDoc {
        final String path;
        final SearchContext sc;
        final int depth;

        public LuceneDoc(String path, SearchContext sc) {
            this.path = path;
            this.sc = sc;
            this.depth = PathUtils.getDepth(path);
        }

        public Iterable<LuceneDoc> getChildren() {
            //Perform a query for immediate child nodes at given path
            BooleanQuery bq = new BooleanQuery();
            bq.add(new BooleanClause(new TermQuery(newAncestorTerm(path)), BooleanClause.Occur.MUST));
            bq.add(new BooleanClause(newDepthQuery(path), BooleanClause.Occur.MUST));

            try {
                TopDocs docs = sc.searcher.search(bq, Integer.MAX_VALUE);
                return getLuceneDocs(docs, sc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static List<LuceneDoc> getLuceneDocs(TopDocs docs, SearchContext sc) throws IOException {
        List<LuceneDoc> result = new ArrayList<LuceneDoc>(docs.scoreDocs.length);
        IndexReader reader = sc.searcher.getIndexReader();
        for (ScoreDoc doc : docs.scoreDocs){
            result.add(new LuceneDoc(getPath(reader, doc), sc));
        }
        return result;
    }

    private static String getPath(IndexReader reader, ScoreDoc doc) throws IOException {
        PathStoredFieldVisitor visitor = new PathStoredFieldVisitor();
        reader.document(doc.doc, visitor);
        return visitor.getPath();
    }

    private static Query newDepthQuery(String path) {
        int depth = PathUtils.getDepth(path) + 1;
        return newDepthQuery(depth);
    }

    private static Query newDepthQuery(int depth) {
        return NumericRangeQuery.newIntRange(FieldNames.PATH_DEPTH, depth, depth, true, true);
    }

    private static String[] createMsg(String msg){
        return new String[] {msg};
    }

    private static class IndexStats {
        static final String[] FIELD_NAMES = new String[]{
                "path",
                "indexSizeStr",
                "indexSize",
                "suggesterSizeStr",
                "suggesterSize",
                "numDocs",
                "maxDoc",
                "numDeletedDocs",
                "nrtIndexSize",
                "nrtIndexSizeStr",
                "nrtNumDocs"
        };

        static final String[] FIELD_DESCRIPTIONS = new String[]{
                "Path",
                "Index size in human readable format",
                "Index size in bytes",
                "Suggester size in human readable format",
                "Suggester size in bytes",
                "Number of documents in this index.",
                "The time and date for when the longest query took place",
                "Number of deleted documents",
                "NRT Index Size in bytes",
                "NRT Index Size in human readable format",
                "Number of documents in NRT index"
        };

        @SuppressWarnings("rawtypes")
        static final OpenType[] FIELD_TYPES = new OpenType[]{
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.LONG,
                SimpleType.STRING,
                SimpleType.LONG,
                SimpleType.INTEGER,
                SimpleType.INTEGER,
                SimpleType.INTEGER,
                SimpleType.LONG,
                SimpleType.STRING,
                SimpleType.INTEGER
        };

        static final CompositeType TYPE = createCompositeType();

        static CompositeType createCompositeType() {
            try {
                return new CompositeType(
                        IndexStats.class.getName(),
                        "Composite data type for Lucene Index statistics",
                        IndexStats.FIELD_NAMES,
                        IndexStats.FIELD_DESCRIPTIONS,
                        IndexStats.FIELD_TYPES);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }

        private final String path;
        private final long indexSize;
        private final int numDocs;
        private final int maxDoc;
        private final int numDeletedDocs;
        private final String indexSizeStr;
        private final long suggesterSize;
        private final String suggesterSizeStr;
        private final long nrtIndexSize;
        private final String nrtIndexSizeStr;
        private final int numDocsNRT;

        public IndexStats(String path, IndexNode indexNode) throws IOException {
            this.path = path;
            numDocs = indexNode.getSearcher().getIndexReader().numDocs();
            maxDoc = indexNode.getSearcher().getIndexReader().maxDoc();
            numDeletedDocs = indexNode.getSearcher().getIndexReader().numDeletedDocs();
            indexSize = getIndexSize(indexNode.getPrimaryReaders());
            indexSizeStr = humanReadableByteCount(indexSize);
            suggesterSize = dirSize(indexNode.getSuggestDirectory());
            suggesterSizeStr = humanReadableByteCount(suggesterSize);
            nrtIndexSize = getIndexSize(indexNode.getNRTReaders());
            numDocsNRT = getNumDocs(indexNode.getNRTReaders());
            nrtIndexSizeStr = humanReadableByteCount(nrtIndexSize);
        }

        CompositeDataSupport toCompositeData() {
            Object[] values = new Object[]{
                    path,
                    indexSizeStr,
                    indexSize,
                    suggesterSizeStr,
                    suggesterSize,
                    numDocs,
                    maxDoc,
                    numDeletedDocs,
                    nrtIndexSize,
                    nrtIndexSizeStr,
                    numDocsNRT
            };
            try {
                return new CompositeDataSupport(TYPE, FIELD_NAMES, values);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static class BadIndexStats {
        static final String[] FIELD_NAMES = new String[]{
                "path",
                "stats",
                "failingSince",
                "exception"
        };

        static final String[] FIELD_DESCRIPTIONS = new String[]{
                "Path",
                "Failure stats",
                "Failure start time",
                "Exception"
        };

        @SuppressWarnings("rawtypes")
        static final OpenType[] FIELD_TYPES = new OpenType[]{
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.STRING,
        };

        static final CompositeType TYPE = createCompositeType();

        static CompositeType createCompositeType() {
            try {
                return new CompositeType(
                        BadIndexStats.class.getName(),
                        "Composite data type for Lucene Bad Index statistics",
                        BadIndexStats.FIELD_NAMES,
                        BadIndexStats.FIELD_DESCRIPTIONS,
                        BadIndexStats.FIELD_TYPES);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }

        private final BadIndexInfo info;

        public BadIndexStats(BadIndexInfo info){
            this.info = info;
        }

        CompositeDataSupport toCompositeData() {
            Object[] values = new Object[]{
                    info.path,
                    info.getStats(),
                    String.format("%tc", info.getCreatedTime()),
                    info.getException(),
            };
            try {
                return new CompositeDataSupport(TYPE, FIELD_NAMES, values);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }
    //~---------------------------------------------------------< Internal >

    private static IndexReader getPrimaryReader(List<LuceneIndexReader> indexReaders) {
        return indexReaders.isEmpty() ? null : indexReaders.get(0).getReader();
    }

    private static long getIndexSize(List<LuceneIndexReader> readers) throws IOException {
        long totalSize = 0;
        for (LuceneIndexReader r : readers){
            totalSize += r.getIndexSize();
        }
        return totalSize;
    }

    private static Directory getDirectory(IndexReader reader) {
        if (reader instanceof DirectoryReader) {
            return ((DirectoryReader) reader).directory();
        }
        return null;
    }

    private static int getNumDocs(List<LuceneIndexReader> readers) {
        int numDoc = 0;
        for (LuceneIndexReader r : readers){
            numDoc += r.getReader().numDocs();
        }
        return numDoc;
    }

}
