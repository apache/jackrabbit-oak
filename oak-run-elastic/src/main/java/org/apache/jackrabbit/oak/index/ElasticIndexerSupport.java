package org.apache.jackrabbit.oak.index;


import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.*;
import java.util.Properties;

public class ElasticIndexerSupport extends IndexerSupport {

    public ElasticIndexerSupport(IndexHelper indexHelper, String checkpoint) {
        super(indexHelper, checkpoint);
    }

    @Override
    public void postIndexWork(NodeStore copyOnWriteStore) throws CommitFailedException, IOException {
        switchIndexLanesBack(copyOnWriteStore);
        dumpIndexDefinitions(copyOnWriteStore);
        createIndexMetaDataForElastic();
    }

    private void createIndexMetaDataForElastic() throws IOException {
        // Need to create this to make have similar meta data file as created by lucene
        // Effectively this will help us reuse the IndexImporter class in a better way without having to rewrite it completely
        // for elastic and have code duplication

        for (String indexPath : indexHelper.getIndexPaths()) {
            File dir = new File(getLocalIndexDir(), indexPath.substring(indexPath.lastIndexOf("/")));
            FileUtils.forceMkdir(dir);
            File infoFile = new File(dir, "index-details.txt");
            infoFile.createNewFile();
            Properties p = new Properties();
            p.setProperty("indexPath", indexPath);
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(infoFile))) {
                p.store(os, "Indexer info");
            }
        }
    }
}
