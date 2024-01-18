package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.IndexAggregationCommonTest;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.junit.ClassRule;

import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;

public class ElasticIndexAggregationTest extends IndexAggregationCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule(
            ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    public ElasticIndexAggregationTest() {
        this.indexOptions = new ElasticIndexOptions();
        this.repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
    }

    @Override
    protected ContentRepository createRepository() {
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    /**
     * <code>
     * <aggregate primaryType="nt:file">
     * <include>jcr:content</include>
     * <include>jcr:content/*</include>
     * <include-property>jcr:content/jcr:lastModified</include-property>
     * </aggregate>
     * <code>
     */
    private static QueryIndex.NodeAggregator getNodeAggregator() {
        return new SimpleNodeAggregator()
                .newRuleWithName(NT_FILE, List.of(JCR_CONTENT, JCR_CONTENT + "/*"))
                .newRuleWithName(NT_FOLDER, List.of("myFile", "subfolder/subsubfolder/file"));
    }
}
