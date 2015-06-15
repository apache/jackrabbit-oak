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
package org.apache.jackrabbit.oak.plugins.document;


import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;

/**
 * Keeps track of changes performed between two consecutive background updates.
 *
 * Done:
 *      Query external changes in chunks.
 *      {@link #getChanges(Revision, Revision, DocumentStore)} current reads
 *      all JournalEntry documents in one go with a limit of Integer.MAX_VALUE.
 * Done:
 *      Use external sort when changes are applied to diffCache. See usage of
 *      {@link #applyTo(DiffCache, Revision, Revision)} in
 *      {@link DocumentNodeStore#backgroundRead(boolean)}.
 *      The utility {@link StringSort} can be used for this purpose.
 * Done:
 *      Push changes to {@link MemoryDiffCache} instead of {@link LocalDiffCache}.
 *      See {@link TieredDiffCache#newEntry(Revision, Revision)}. Maybe a new
 *      method is needed for this purpose?
 * Done (incl junit) 
 *      Create JournalEntry for external changes related to _lastRev recovery.
 *      See {@link LastRevRecoveryAgent#recover(Iterator, int, boolean)}.
 * Done (incl junit)
 *      Cleanup old journal entries in the document store.
 * Done:
 *      integrate the JournalGarbageCollector similarly to the VersionGarbageCollector
 */
public final class JournalEntry extends Document {

    private static final Logger LOG = LoggerFactory.getLogger(JournalEntry.class);

    /**
     * The revision format for external changes:
     * &lt;clusterId>-&lt;timestamp>-&lt;counter>. The string is prefixed with
     * "b" if it denotes a branch revision.
     */
    private static final String REVISION_FORMAT = "%d-%0" +
            Long.toHexString(Long.MAX_VALUE).length() + "x-%0" +
            Integer.toHexString(Integer.MAX_VALUE).length() + "x";

    private static final String CHANGES = "_c";

    private static final String BRANCH_COMMITS = "_bc";

    private static final int READ_CHUNK_SIZE = 1024;
    
    private static final int STRINGSORT_OVERFLOW_TO_DISK_THRESHOLD = 1024 * 1024; // switch to disk after 1MB
    
    private final DocumentStore store;

    private volatile TreeNode changes = null;

    JournalEntry(DocumentStore store) {
        this.store = store;
    }
    
    static StringSort newSorter() {
        return new StringSort(STRINGSORT_OVERFLOW_TO_DISK_THRESHOLD, new Comparator<String>() {
            @Override
            public int compare(String arg0, String arg1) {
                return arg0.compareTo(arg1);
            }
        });
    }
	
    static void applyTo(@Nonnull StringSort externalSort,
    				    @Nonnull DiffCache diffCache,
    				    @Nonnull Revision from,
    				    @Nonnull Revision to) throws IOException {
        LOG.debug("applyTo: starting for {} to {}", from, to);
		externalSort.sort();
		// note that it is not deduplicated yet
		LOG.debug("applyTo: sorting done.");
		
		final DiffCache.Entry entry = checkNotNull(diffCache).newEntry(from, to, false);

		final Iterator<String> it = externalSort.getIds();
		if (!it.hasNext()) {
			// nothing at all? that's quite unusual..
			
			// we apply this diff as one '/' to the entry then
		    entry.append("/", "");
			entry.done();
			return;
		}
		String previousPath = it.next();
		TreeNode node = new TreeNode(null, "");
		node = node.getOrCreatePath(previousPath); 
		int totalCnt = 0;
		int deDuplicatedCnt = 0;
		while(it.hasNext()) {
			totalCnt++;
			final String currentPath = it.next();
			if (previousPath.equals(currentPath)) {
				// de-duplication
				continue;
			}
			
			// 'node' contains one hierarchy line, eg /a, /a/b, /a/b/c, /a/b/c/d
			// including the children on each level.
			// these children have not yet been appended to the diffCache entry
			// and have to be added as soon as the 'currentPath' is not
			// part of that hierarchy anymore and we 'move elsewhere'.
			// eg if 'currentPath' is /a/b/e, then we must flush /a/b/c/d and /a/b/c
			while(node!=null && !node.isParentOf(currentPath)) {
				// add parent to the diff entry
			    entry.append(node.getPath(), getChanges(node));
				deDuplicatedCnt++;
				node = node.parent;
			}
			
			if (node==null) {
			    // we should never go 'passed' the root, hence node should 
			    // never be null - if it becomes null anyway, start with
			    // a fresh root:
			    node = new TreeNode(null, "");
	            node = node.getOrCreatePath(currentPath);
			} else {
			    // this is the normal route: we add a direct or grand-child
			    // node to the current node:
			    node = node.getOrCreatePath(currentPath);
			}
			previousPath = currentPath;
		}
		
		// once we're done we still have the last hierarchy line contained in 'node',
		// eg /x, /x/y, /x/y/z
		// and that one we must now append to the diffcache entry:
		while(node!=null) {
            entry.append(node.getPath(), getChanges(node));
			deDuplicatedCnt++;
			node = node.parent;
		}
		
		// and finally: mark the diffcache entry as 'done':
        entry.done();
        LOG.debug("applyTo: done. totalCnt: {}, deDuplicatedCnt: {}", totalCnt, deDuplicatedCnt);
    }
    
    /**
     * Reads all external changes between the two given revisions (with the same clusterId)
     * from the journal and appends the paths therein to the provided sorter.
     *
     * @param sorter the StringSort to which all externally changed paths between
     * the provided revisions will be added
     * @param from the lower bound of the revision range (exclusive).
     * @param to the upper bound of the revision range (inclusive).
     * @param store the document store to query.
     * @throws IOException 
     */
    static void fillExternalChanges(@Nonnull StringSort sorter,
                                    @Nonnull Revision from,
                                    @Nonnull Revision to,
                                    @Nonnull DocumentStore store) throws IOException {
        checkArgument(checkNotNull(from).getClusterId() == checkNotNull(to).getClusterId());
        
        // to is inclusive, but DocumentStore.query() toKey is exclusive
        final String inclusiveToId = asId(to);
        to = new Revision(to.getTimestamp(), to.getCounter() + 1,
                to.getClusterId(), to.isBranch());

        // read in chunks to support very large sets of changes between subsequent background reads
        // to do this, provide a (TODO eventually configurable) limit for the number of entries to be returned per query
        // if the number of elements returned by the query is exactly the provided limit, then
        // loop and do subsequent queries
        final String toId = asId(to);
        String fromId = asId(from);
        while(true) {
        	if (fromId.equals(inclusiveToId)) {
        		// avoid query if from and to are off by just 1 counter (which we do due to exclusiveness of query borders)
        		// as in this case the query will always be empty anyway - so avoid doing the query in the first place
        		break;
        	}
			List<JournalEntry> partialResult = store.query(JOURNAL, fromId, toId, READ_CHUNK_SIZE);
			if (partialResult==null) {
				break;
			}
			for(JournalEntry d: partialResult) {
				d.addTo(sorter);
			}
			if (partialResult.size()<READ_CHUNK_SIZE) {
				break;
			}
			// otherwise set 'fromId' to the last entry just processed
			// that works fine as the query is non-inclusive (ie does not include the from which we'd otherwise double-process)
			fromId = partialResult.get(partialResult.size()-1).getId();
        }
    }

    long getRevisionTimestamp() {
        final String[] parts = getId().split("-");
        return Long.parseLong(parts[1], 16);
    }

    void modified(String path) {
        TreeNode node = getChanges();
        for (String name : PathUtils.elements(path)) {
            node = node.getOrCreate(name);
        }
    }

    void modified(Iterable<String> paths) {
        for (String p : paths) {
            modified(p);
        }
    }

    void branchCommit(@Nonnull Iterable<Revision> revisions) {
        String branchCommits = (String) get(BRANCH_COMMITS);
        if (branchCommits == null) {
            branchCommits = "";
        }
        for (Revision r : revisions) {
            if (branchCommits.length() > 0) {
                branchCommits += ",";
            }
            branchCommits += asId(r.asBranchRevision());
        }
        put(BRANCH_COMMITS, branchCommits);
    }

    String getChanges(String path) {
        TreeNode node = getNode(path);
        if (node == null) {
            return "";
        }
        return getChanges(node);
    }

    UpdateOp asUpdateOp(@Nonnull Revision revision) {
        String id = asId(revision);
        UpdateOp op = new UpdateOp(id, true);
        op.set(ID, id);
        op.set(CHANGES, getChanges().serialize());
        String bc = (String) get(BRANCH_COMMITS);
        if (bc != null) {
            op.set(BRANCH_COMMITS, bc);
        }
        return op;
    }
    
    void addTo(final StringSort sort) throws IOException {
        TreeNode n = getChanges();
        TraversingVisitor v = new TraversingVisitor() {
            
            @Override
            public void node(TreeNode node, String path) throws IOException {
                sort.add(path);
            }
        };
		n.accept(v, "/");
        for (JournalEntry e : getBranchCommits()) {
            e.getChanges().accept(v, "/");
        }
    }

    /**
     * Returns the branch commits that are related to this journal entry.
     *
     * @return the branch commits.
     */
    @Nonnull
    Iterable<JournalEntry> getBranchCommits() {
        List<JournalEntry> commits = Lists.newArrayList();
        String bc = (String) get(BRANCH_COMMITS);
        if (bc != null) {
            for (String id : bc.split(",")) {
                JournalEntry d = store.find(JOURNAL, id);
                if (d == null) {
                    throw new IllegalStateException(
                            "Missing external change for branch revision: " + id);
                }
                //TODO: could this also be a problem with very large number of branches ???
                commits.add(d);
            }
        }
        return commits;
    }

    //-----------------------------< internal >---------------------------------

    private static String getChanges(TreeNode node) {
        JsopBuilder builder = new JsopBuilder();
        for (String name : node.keySet()) {
            builder.tag('^');
            builder.key(name);
            builder.object().endObject();
        }
        return builder.toString();
    }

    static String asId(@Nonnull Revision revision) {
        checkNotNull(revision);
        String s = String.format(REVISION_FORMAT, revision.getClusterId(), revision.getTimestamp(), revision.getCounter());
        if (revision.isBranch()) {
            s = "b" + s;
        }
        return s;
    }

    @CheckForNull
    private TreeNode getNode(String path) {
        TreeNode node = getChanges();
        for (String name : PathUtils.elements(path)) {
            node = node.get(name);
            if (node == null) {
                return null;
            }
        }
        return node;
    }

    @Nonnull
    private TreeNode getChanges() {
        if (changes == null) {
            TreeNode node = new TreeNode(null, "");
            String c = (String) get(CHANGES);
            if (c != null) {
                node.parse(new JsopTokenizer(c));
            }
            changes = node;
        }
        return changes;
    }

    private static final class TreeNode {

        private final Map<String, TreeNode> children = Maps.newHashMap();

        private final String path;
        private final TreeNode parent;
        
        TreeNode(TreeNode parent, String name) {
            if (name.contains("/")) {
                throw new IllegalArgumentException("name must not contain /: "+name);
            }
            this.parent = parent;
            if (parent==null) {
                this.path = "/";
            } else if (parent.parent==null) {
                this.path = "/" + name;
            } else {
                this.path = parent.path + "/" + name;
            }
        }
        
        public TreeNode getOrCreatePath(String path) {
            if (path.equals(this.path)) {
                // then path denotes the same as myself, hence return myself
                return this;
            }
            if (!path.startsWith(this.path)) {
                // this must never happen
                throw new IllegalStateException("path not child of myself. path: "+path+", myself: "+this.path);
            }
            String sub = this.path.equals("/") ? path.substring(1) : path.substring(this.path.length()+1);
            String[] parts = sub.split("/");
            TreeNode n = this;
            for (int i = 0; i < parts.length; i++) {
                if (parts[i]!=null && parts[i].length()>0) {
                    n = n.getOrCreate(parts[i]);
                }
            }
            return n;
        }

        public boolean isParentOf(String path) {
            if (this.path.equals("/")) {
                // root is parent of everything
                return true;
            }
            if (!path.startsWith(this.path+"/")) {
                // then I'm not parent of that path
                return false;
            }
            final String sub = path.substring(this.path.length()+1);
            if (sub.indexOf("/", 1)!=-1) {
                // if the 'sub' part contains a / then 
                // it is not a direct child of myself,
                // so I'm a grand-parent but not a direct-parent
                return false;
            }
            return true;
        }

        private String getPath() {
            return path;
        }

        void parse(JsopReader reader) {
            reader.read('{');
            if (!reader.matches('}')) {
                do {
                    String name = Utils.unescapePropertyName(reader.readString());
                    reader.read(':');
                    getOrCreate(name).parse(reader);
                } while (reader.matches(','));
                reader.read('}');
            }
        }

        String serialize() {
            JsopBuilder builder = new JsopBuilder();
            builder.object();
            toJson(builder);
            builder.endObject();
            return builder.toString();
        }

        @Nonnull
        Set<String> keySet() {
            return children.keySet();
        }

        @CheckForNull
        TreeNode get(String name) {
            return children.get(name);
        }

        void accept(TraversingVisitor visitor, String path) throws IOException {
            visitor.node(this, path);
            for (Map.Entry<String, TreeNode> entry : children.entrySet()) {
                entry.getValue().accept(visitor, concat(path, entry.getKey()));
            }
        }

        private void toJson(JsopBuilder builder) {
            for (Map.Entry<String, TreeNode> entry : children.entrySet()) {
                builder.key(Utils.escapePropertyName(entry.getKey()));
                builder.object();
                entry.getValue().toJson(builder);
                builder.endObject();
            }
        }

        private TreeNode getOrCreate(String name) {
            TreeNode c = children.get(name);
            if (c == null) {
                c = new TreeNode(this, name);
                children.put(name, c);
            }
            return c;
        }
    }

    private interface TraversingVisitor {

        void node(TreeNode node, String path) throws IOException;
    }

}
