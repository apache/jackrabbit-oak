package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.IndexExclusionQueryCommonTest;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.junit.ClassRule;

public class ElasticIndexExclusionQueryCommonTest extends IndexExclusionQueryCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule(
            ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    @Override
    protected ContentRepository createRepository() {
        indexOptions = new ElasticIndexOptions();
        ElasticTestRepositoryBuilder builder = new ElasticTestRepositoryBuilder(elasticRule);
        builder.setNodeStore(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT));
        repositoryOptionsUtil = builder.build();

        return repositoryOptionsUtil.getOak().createContentRepository();
    }
}
