package org.apache.jackrabbit.oak.indexversion;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PurgeOldIndexVersion {
    private static final Logger log = LoggerFactory.getLogger(PurgeOldIndexVersion.class);
    private boolean isReadWriteRepository = false;
    private long threshold;
    private List<String> indexPaths;
    private long DEFAULT_THRESHOLD = 432000000l; // 5 days in millis

    public void execute(String... args) throws Exception {
        Options opts = parseCommandLineParams(args);
        this.isReadWriteRepository = opts.getCommonOpts().isReadWrite();
        if (!isReadWriteRepository) {
            log.info("Repository connected in read-only mode. Use '--read-write' for write operations");
        }
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            NodeStore nodeStore = fixture.getStore();
            Set<String> indexPathSet = filterIndexPaths(getIndexPathList(nodeStore));
            Map<String, Set<String>> segregateIndexes = segregateIndexes(indexPathSet);
            for (Map.Entry<String, Set<String>> entry : segregateIndexes.entrySet()) {
                String trimmedBaseIndexPath = PurgeOldVersionUtils.trimSlash(entry.getKey());
                String trimmedIndexParentPath = trimmedBaseIndexPath.substring(0, trimmedBaseIndexPath.lastIndexOf('/'));
                List<IndexName> indexNameObjectList = getIndexNameObjectList(entry.getValue());
                NodeState indexDefParentNode = NodeStateUtils.getNode(nodeStore.getRoot(),
                        "/" + trimmedIndexParentPath);
                List<IndexVersionOperation> toDeleteIndexNameObjectList =
                        IndexVersionOperation.applyOperation(indexDefParentNode, indexNameObjectList, this.threshold);
                if (isReadWriteRepository) {
                    purgeOldIndexVersion(nodeStore, toDeleteIndexNameObjectList, trimmedBaseIndexPath);
                } else {
                    log.info("Repository is opened in read-write mode: IndexOperations" +
                            " for index at path {} are : {}", trimmedBaseIndexPath, toDeleteIndexNameObjectList);
                }
            }
        }
    }

    private Map<String, Set<String>> segregateIndexes(Set<String> indexPathSet) {
        Map<String, Set<String>> segregatedIndexes = new HashMap<>();
        for (String path : indexPathSet) {
            String baseIndexPath = getBaseIndexPath(path);
            Set<String> indexPaths = segregatedIndexes.get(baseIndexPath);
            if (indexPaths == null) {
                indexPaths = new HashSet<>();
            }
            indexPaths.add(PurgeOldVersionUtils.trimSlash(path));
            segregatedIndexes.put(baseIndexPath, indexPaths);
        }
        return segregatedIndexes;
    }

    private Iterable<String> getIndexPathList(NodeStore store) throws CommitFailedException, IOException {
        IndexPathService indexPathService = new IndexPathServiceImpl(store);
        Iterable<String> indexPaths = indexPathService.getIndexPaths();
        return indexPaths;
    }

    private Set<String> filterIndexPaths(Iterable<String> indexPathList) {
        Set<String> filteredIndexPaths = new HashSet<>();
        for (String indexPathOption : this.indexPaths) {
            for (String indexPath : indexPathList) {
                if (indexPath.startsWith(indexPathOption)) {
                    filteredIndexPaths.add(PurgeOldVersionUtils.trimSlash(indexPath));
                }
            }
        }
        return filteredIndexPaths;
    }

    StringBuilder stringJoiner(String[] stringArray, int arrayIndexToJoin, String delimiter) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int index = 0; index <= arrayIndexToJoin; index++) {
            stringBuilder.append(stringArray[index]);
            stringBuilder.append(delimiter);
        }
        return stringBuilder;
    }

    private String getBaseIndexPath(String path) {
        String trimmedPath = PurgeOldVersionUtils.trimSlash(path);
        String[] fragmentedPath = trimmedPath.split("/");
        String indexName = fragmentedPath[fragmentedPath.length - 1];
        String baseIndexName = PurgeOldVersionUtils.getBaseIndexName(indexName);
        return stringJoiner(fragmentedPath,
                fragmentedPath.length - 2, "/")
                .append(baseIndexName).toString();
    }

    private Set<String> getIndexVersionNames(Set<String> versionedIndexPaths) {
        Set<String> indexVersionNames = new HashSet<>();
        for (String indexPath : versionedIndexPaths) {
            String trimmedIndexPath = PurgeOldVersionUtils.trimSlash(indexPath);
            String[] splitPathArray = trimmedIndexPath.split("/");
            String indexName = splitPathArray[splitPathArray.length - 1];
            indexVersionNames.add(indexName);
        }
        return indexVersionNames;
    }

    private List<IndexName> getIndexNameObjectList(Set<String> versionedIndexPaths) {
        Set<String> allIndexNameVersions = getIndexVersionNames(versionedIndexPaths);
        List<IndexName> indexNameObjectList = getVersionedIndexNameObjects(allIndexNameVersions);
        return indexNameObjectList;
    }

    private void purgeOldIndexVersion(NodeStore store,
                                      List<IndexVersionOperation> toDeleteIndexNameObjectList, String trimmedIndexPath) throws CommitFailedException {
        String indexPathParent = "/" + trimmedIndexPath.substring(0, trimmedIndexPath.lastIndexOf('/'));
        for (IndexVersionOperation toDeleteIndexNameObject : toDeleteIndexNameObjectList) {
            NodeState root = store.getRoot();
            NodeBuilder rootBuilder = root.builder();
            NodeBuilder nodeBuilder = PurgeOldVersionUtils.getNode(rootBuilder, indexPathParent + "/" + toDeleteIndexNameObject.getIndexName().getNodeName());
            if (nodeBuilder.exists()) {
                if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE_HIDDEN_AND_DISABLE) {
                    nodeBuilder.setProperty("type", "disabled", Type.STRING);
                    PurgeOldVersionUtils.recursiveDeleteHiddenChildNodes(store, trimmedIndexPath.substring(0, trimmedIndexPath.lastIndexOf('/')) + "/" + toDeleteIndexNameObject.getIndexName().getNodeName());
                } else if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE) {
                    nodeBuilder.remove();
                }
                EditorHook hook = new EditorHook(
                        new IndexUpdateProvider(new PropertyIndexEditorProvider()));
                store.merge(rootBuilder, hook, CommitInfo.EMPTY);
            } else {
                log.error("nodebuilder null for path " + indexPathParent + "/" + toDeleteIndexNameObject.getIndexName().getNodeName());
            }
        }
    }

    private List<IndexName> getVersionedIndexNameObjects(Set<String> allIndexes) {
        List<IndexName> indexNameObjectList = new ArrayList<>();
        for (String indexNameString : allIndexes) {
            indexNameObjectList.add(IndexName.parse(indexNameString));
        }
        return indexNameObjectList;
    }

    private Options parseCommandLineParams(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Long> thresholdOption = parser.accepts("threshold")
                .withOptionalArg().ofType(Long.class).defaultsTo(DEFAULT_THRESHOLD);
        OptionSpec<String> indexPathsOption = parser.accepts("index-paths", "Comma separated list of index paths for which the " +
                "selected operations need to be performed")
                .withOptionalArg().ofType(String.class).withValuesSeparatedBy(",").defaultsTo("/oak:index");
        Options opts = new Options();
        OptionSet optionSet = opts.parseAndConfigure(parser, args);
        this.threshold = optionSet.valueOf(thresholdOption);
        this.indexPaths = optionSet.valuesOf(indexPathsOption);
        return opts;
    }
}
