package org.apache.jackrabbit.oak.run;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.MongoClient;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class ReduceToPathsCommand implements Command {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ReduceToPathsCommand.class);
    
    private static final Pattern BLOB_ID_PROPERTY = Pattern.compile("^\":blobId:([a-zA-Z0-9]+)\"$");
    
    public static void main(String[] args) throws Exception {
        new ReduceToPathsCommand().execute(args);
    }
    
    private Map<String, Integer> remappedRevisions = Maps.newHashMap();
    private Set<String> blobs = Sets.newHashSet();

    private GarbageCollectableBlobStore blobStore;

    private List<String> pathValues;

    @Override
    public void execute(String... args) throws Exception {
        
        // TODO - support RDB as well
        OptionParser parser = new OptionParser();

        // mongo specific options:
        OptionSpec<String> host = parser.accepts("host", "MongoDB host").withRequiredArg().defaultsTo("127.0.0.1");
        OptionSpec<Integer> port = parser.accepts("port", "MongoDB port").withRequiredArg().ofType(Integer.class).defaultsTo(27017);
        OptionSpec<String> dbName = parser.accepts("db", "MongoDB database").withRequiredArg().defaultsTo("oak");
        OptionSpec<String> paths = parser.accepts("paths", "Paths (at least one is required)").withRequiredArg().ofType(String.class).withValuesSeparatedBy(',');

        OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
        OptionSet options = parser.parse(args);

        pathValues = paths.values(options);
        
        if (options.has(help) || pathValues.isEmpty()) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        for (String path : pathValues) {
            checkArgument(PathUtils.getDepth(path) == 1, "Path %s has depth != 1", path);
        }
        
        LOGGER.info("Starting up with paths to keep: {}", pathValues);

        MongoClient client = new MongoClient(host.value(options), port.value(options));
        final DocumentNodeStore store = new DocumentMK.Builder().setMongoDB(client.getDB(dbName.value(options)))
                .getNodeStore();
        
        if ( store.getBlobStore() instanceof GarbageCollectableBlobStore ) {
            blobStore = (GarbageCollectableBlobStore) store.getBlobStore();
        } else {
            LOGGER.warn("Blob store {} is not an instance of {}, will not cleanup blobs", 
                    store.getBlobStore(), GarbageCollectableBlobStore.class.getName());
        }

        try {
            for (NodeDocument childDoc : getAllChildren(store, "/")) {
                if (pathValues.contains(childDoc.getMainPath())) {
                    LOGGER.info("Will keep document at {}", childDoc.getPath());
                    checkCommitRoot(childDoc, store);
                } else {
                    // TODO - what about indexes and ACLS?
                    LOGGER.info("Will remove document at " + childDoc.getPath());
                    store.getDocumentStore().remove(Collection.NODES, childDoc.getId());
                }
            }
            
            LOGGER.info("Cleaning up root node");
            store.getDocumentStore().remove(Collection.NODES, "0:/");

            LOGGER.info("Calculating blobs to remove - keeping {}", blobs.size());
            Iterator<String> allChunks = blobStore.getAllChunkIds(0);
            List<String> blobsToDelete = Lists.newArrayList();
            while ( allChunks.hasNext() ) {
                String chunkId = allChunks.next();
                if ( blobs.contains(chunkId)) {
                    continue;
                }
                blobsToDelete.add(chunkId);
            }
            
            LOGGER.info("Deleting {} blobs", blobsToDelete);
            
            blobStore.countDeleteChunks(blobsToDelete, 0);
            
            LOGGER.info("Execution complete");            
        } finally {
            store.dispose();
        }

    }

    private void checkCommitRoot(NodeDocument doc, DocumentNodeStore store) {

        // TODO - what happens if there are two separate sub-trees which both need
        // to re-root the same revision?
        for (Revision revision : doc.getAllChanges()) {
            if (!doc.isCommitted(revision)) {
                continue;
            }
            
            String commitRootPath = doc.getCommitRootPath(revision);
            if ( commitRootPath == null ) {
                continue;
            }
            
            boolean shouldReroot = false;
            for ( String path : pathValues ) {
                if ( PathUtils.isAncestor(commitRootPath, path) ) {
                    shouldReroot = true;
                    break;
                }
            }
            
            if ( !shouldReroot ) {
                continue;
            }
            
            UpdateOp update = new UpdateOp(doc.getId(), false);

            Integer commitRootDepth = remappedRevisions.get(revision.toString());
            
            if ( commitRootDepth == null ) {
                // first time we encounter the commit
                // breadth-first search -> the ideal candidate for the new commit root
                update.setMapEntry(NodeDocument.REVISIONS, revision, "c");
                update.removeMapEntry(NodeDocument.COMMIT_ROOT, revision);
                store.getDocumentStore().createOrUpdate(Collection.NODES, update);
                
                LOGGER.info("Document {}, revision {}: setting in _revisions as committed, removing from _commitRoot", doc.getId(), revision);
                
                remappedRevisions.put(revision.toString(), PathUtils.getDepth(doc.getPath()));
            } else {
                
                update.setMapEntry(NodeDocument.COMMIT_ROOT, revision, commitRootDepth.toString());
                
                LOGGER.info("Document {}, revision {}: setting new _commitRoot depth to ", doc.getId(), revision, commitRootDepth);
            }
            
            if ( update.hasChanges() ) {
                store.getDocumentStore().createOrUpdate(Collection.NODES, update);
            }
            
            if ( blobStore != null ) {
                if ( Long.valueOf(NodeDocument.HAS_BINARY_VAL).equals(doc.get(NodeDocument.HAS_BINARY_FLAG))) {
                    
                    Map<Revision,String> jcrData = doc.getValueMap("jcr:data");
                    for ( String blob : jcrData.values()) {
                        if ( !doc.isCommitted(revision)) {
                            continue;
                        }
                        
                        Matcher matcher = BLOB_ID_PROPERTY.matcher(blob);
                        
                        if ( !matcher.matches()) {
                            LOGGER.warn("Found malformed blob reference: {} for document at path {}, skipping.", blob, doc.getPath());
                            continue;
                        }
                        
                        Iterator<String> blobIdIterator;
                        try {
                            blobIdIterator = blobStore.resolveChunks(matcher.group(1));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        while ( blobIdIterator.hasNext() ) {
                            blobs.add(blobIdIterator.next());
                        }
                    }
                    
                }
            }
        }
        
        for (NodeDocument child : getAllChildren(store, doc.getMainPath())) {
            checkCommitRoot(child, store);
        }
    }

    private List<NodeDocument> getAllChildren(final DocumentNodeStore store, String path) {

        return store.getDocumentStore().query(Collection.NODES, Utils.getKeyLowerLimit(path),
                Utils.getKeyUpperLimit(path), Integer.MAX_VALUE);
    }
}
