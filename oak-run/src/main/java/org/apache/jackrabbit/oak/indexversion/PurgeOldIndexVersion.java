package org.apache.jackrabbit.oak.indexversion;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.index.IndexOptions;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PurgeOldIndexVersion {
    private static final Logger log = LoggerFactory.getLogger(PurgeOldIndexVersion.class);
    Options opts = null;
    private OptionParser parser = new OptionParser();

    public void execute(String... args) throws Exception {
        opts = parseCommandLineParams(args);
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            NodeStore nodeStore = fixture.getStore();
            List<String> indexPathList = getIndexPathList(nodeStore);
            Map<String, List<String>> segregateIndexes = segregateIndexes(indexPathList);
            execute2(nodeStore, segregateIndexes);
        }
    }

    private Map<String, List<String>> segregateIndexes(List<String> indexPathList) {

        Map<String, List<String>> segregatedIndexes = new HashMap<>();
        for (String path : indexPathList) {
            String baseIndexPath = getBaseIndexPath(path);
            List<String> indexPaths = segregatedIndexes.get(baseIndexPath);
            if (indexPaths == null) {
                indexPaths = new LinkedList<>();
            }
            indexPaths.add(PurgeOldVersionUtils.trimSlash(path));
            segregatedIndexes.put(baseIndexPath, indexPaths);
        }
        return segregatedIndexes;
    }

    private List<String> getIndexPathList(NodeStore store) throws CommitFailedException, IOException {
        IndexPathService indexPathService = new IndexPathServiceImpl(store);

        Iterable<String> indexPaths = indexPathService.getIndexPaths();
        List<String> indexPathsList = new LinkedList<>();
        for (String indexPath : indexPaths) {
            if (indexPath.startsWith("/oak:index"))
            indexPathsList.add(PurgeOldVersionUtils.trimSlash(indexPath));
        }
        return indexPathsList;
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

    private void execute2(NodeStore store, Map<String, List<String>> segregateIndexes) throws CommitFailedException {
        for (Map.Entry<String, List<String>> entry : segregateIndexes.entrySet()) {
            execute3(store, entry.getValue());
        }
    }

    private List<String> getIndexNameVersionList(List<String> versionedIndexPathList) {
        List<String> indexNameVersionList = new LinkedList<>();
        for (String indexPath : versionedIndexPathList) {
            String trimmedIndexPath = PurgeOldVersionUtils.trimSlash(indexPath);
            String[] splitPathArray = trimmedIndexPath.split("/");
            String indexName = splitPathArray[splitPathArray.length - 1];
            indexNameVersionList.add(indexName);
        }
        return indexNameVersionList;
    }

    private void execute3(NodeStore store, List<String> versionedIndexPathList) throws CommitFailedException {
        if (versionedIndexPathList.size() != 0) {
            log.info("Versioned list present for index " + versionedIndexPathList.get(0));
            String trimmedIndexPath = PurgeOldVersionUtils.trimSlash(versionedIndexPathList.get(0));
            String[] splitPathArray = trimmedIndexPath.split("/");
            NodeState indexDefParentNode = NodeStateUtils.getNode(store.getRoot(),
                    "/" + trimmedIndexPath.substring(0, trimmedIndexPath.lastIndexOf('/'))); //getIndexDefinitionNodeParentFromPath(store, trimmedIndexPath);
            String indexName = splitPathArray[splitPathArray.length - 1];
            String baseIndexName = PurgeOldVersionUtils.getBaseIndexName(indexName);
//            Iterable<String> allIndexes = getAllIndexes(indexDefParentNode);
            //List<String> allIndexNameVersions = getAllIndexVersions(baseIndexName, allIndexes);
            List<String> allIndexNameVersions = getIndexNameVersionList(versionedIndexPathList);
            List<IndexName> sortedIndexNameObjectList = getVersionedIndexNameObjects(allIndexNameVersions);
            List<IndexVersionOperation> toDeleteIndexNameObjectList =
                    IndexVersionOperation.apply(indexDefParentNode, sortedIndexNameObjectList, 1);
            if (opts.getCommonOpts().isReadWrite()) {
                purgeOldIndexVersion(store, toDeleteIndexNameObjectList, trimmedIndexPath);
            } else {
                log.info("Repository is opened in read-write mode: IndexOperations" +
                        " for index at path {} are : {}", trimmedIndexPath, toDeleteIndexNameObjectList);
            }
        }

    }


    private void purgeOldIndexVersion(NodeStore store,
                                      List<IndexVersionOperation> toDeleteIndexNameObjectList, String trimmedIndexPath) throws CommitFailedException {
        String indexPathParent = "/" + trimmedIndexPath.substring(0, trimmedIndexPath.lastIndexOf('/'));
        for (IndexVersionOperation toDeleteIndexNameObject : toDeleteIndexNameObjectList) {
            NodeState root = store.getRoot();
            NodeBuilder rootBuilder = root.builder();
//            NodeBuilder nodeBuilderParent = PurgeOldVersionUtils.getNode(rootBuilder, indexPathParent + "/" + toDeleteIndexNameObject.getNodeName());
            NodeBuilder nodeBuilder = PurgeOldVersionUtils.getNode(rootBuilder, indexPathParent + "/" + toDeleteIndexNameObject.getIndexName().getNodeName());
            if (nodeBuilder.exists()) {

                if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.NOOP) {

                } else if (toDeleteIndexNameObject.getOperation() == IndexVersionOperation.Operation.DELETE_HIDDEN_AND_DISABLE) {
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
        store.getRoot();

    }

    private List<IndexName> getVersionedIndexNameObjects(List<String> allIndexes) {
        List<IndexName> indexNameObjectList = new ArrayList<>();
        for (String indexNameString : allIndexes) {
            indexNameObjectList.add(IndexName.parse(indexNameString));
        }
        return indexNameObjectList;
    }

    private boolean quiet;

    private Options parseCommandLineParams(String... args) throws Exception {
        OptionParser parser = new OptionParser();
//        OptionSpec<Void> quietOption = parser.accepts("quiet", "be less chatty");
        OptionSpec<Void> indexPathsOption = parser.accepts("index-paths", "index paths for indexes");
        Options opts = new Options();
        OptionSet options = opts.parseAndConfigure(parser, args);
//        quiet = options.has(quietOption);
        boolean isReadWrite = opts.getCommonOpts().isReadWrite();
//        String indexPaths = opts.getOptionSet().;
        boolean success = true;
        if (!isReadWrite) {
            log.debug("Repository connected in read-only mode. Use '--read-write' for write operations");
        }
//        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
//            NodeStore nodeStore = fixture.getStore();
//            return nodeStore;
//        }
        return opts;
    }


}
