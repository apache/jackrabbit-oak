package org.apache.jackrabbit.oak.index;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock;
import org.apache.jackrabbit.oak.plugins.index.importer.ClusterNodeStoreLock;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public abstract class IndexImporterSupportBase {

    protected final NodeStore nodeStore;
    protected final IndexHelper indexHelper;

    public IndexImporterSupportBase(IndexHelper indexHelper) {
        this.nodeStore = indexHelper.getNodeStore();
        this.indexHelper = indexHelper;
    }

    public void importIndex(File importDir) throws IOException, CommitFailedException {
        IndexImporter importer = new IndexImporter(nodeStore, importDir, createIndexEditorProvider(), createLock());
        addImportProviders(importer);
        importer.importIndex();
    }

    private AsyncIndexerLock createLock() {
        if (nodeStore instanceof Clusterable) {
            return new ClusterNodeStoreLock(nodeStore);
        }
        //For oak-run usage with non Clusterable NodeStore indicates that NodeStore is not
        //active. So we can use a noop lock implementation as there is no concurrent run
        return AsyncIndexerLock.NOOP_LOCK;
    }

    protected abstract IndexEditorProvider createIndexEditorProvider() throws IOException;

    protected abstract void addImportProviders(IndexImporter importer);


}
