package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.OrderByCommonTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class ElasticOrderByCommonTest extends OrderByCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    public ElasticOrderByCommonTest() {
        indexOptions = new ElasticIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
    }

    @Test
    @Override
    @Ignore("No support for includePropertyNames/orderedProps in elastic")
    public void sortQueriesWithLong_NotIndexed() throws Exception {
        super.sortQueriesWithLong_NotIndexed();
    }
}
