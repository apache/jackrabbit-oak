package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.StrictPathRestriction;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.TestRepository;
import org.apache.jackrabbit.oak.plugins.index.TestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;

public class LuceneTestRepositoryBuilder extends TestRepositoryBuilder {

    private ResultCountingIndexProvider resultCountingIndexProvider;
    private TestUtil.OptionalEditorProvider optionalEditorProvider;

    public LuceneTestRepositoryBuilder(ExecutorService executorService, TemporaryFolder temporaryFolder) {
        IndexCopier copier = null;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        this.indexProvider = new LuceneIndexProvider(copier);
        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                editorProvider,
                new NodeCounterEditorProvider()
        )));

        resultCountingIndexProvider = new ResultCountingIndexProvider(indexProvider);
        queryEngineSettings = new QueryEngineSettings();
        queryEngineSettings.setStrictPathRestriction(StrictPathRestriction.ENABLE.name());
        optionalEditorProvider = new TestUtil.OptionalEditorProvider();

        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
    }

    public TestRepository build() {
        Oak oak = new Oak(nodeStore)
                .with(getInitialContent())
                .with(securityProvider)
                .with(resultCountingIndexProvider)
                .with((Observer) indexProvider)
                .with(editorProvider)
                .with(optionalEditorProvider)
                .with(indexEditorProvider)
                .with(queryIndexProvider)
                .with(queryEngineSettings);
        if (isAsync) {
            oak.withAsyncIndexing("async", defaultAsyncIndexingTimeInSeconds);
        }
        return new TestRepository(oak).with(isAsync).with(asyncIndexUpdate);
    }
}
