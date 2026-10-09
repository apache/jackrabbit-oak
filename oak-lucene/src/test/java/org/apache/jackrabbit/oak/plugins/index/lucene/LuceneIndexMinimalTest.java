package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class LuceneIndexMinimalTest extends AbstractQueryTest {
    @Override protected void createTestIndexNode() throws Exception { setTraversalEnabled(false); }

    @Override
    protected ContentRepository createRepository() {
        LuceneIndexProvider provider = new LuceneIndexProvider();
        return new Oak().with(new InitialContent()).with(new OpenSecurityProvider())
            .with((QueryIndexProvider) provider).with((Observer) provider)
            .with(new LuceneIndexEditorProvider()).createContentRepository();
    }

    @Test
    public void singleCommit() throws Exception {
        // Index + content in ONE commit
        Tree def = root.getTree("/oak:index").addChild("testIdx");
        def.setProperty("jcr:primaryType", INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, List.of("title"), Type.STRINGS));

        Tree page = root.getTree("/").addChild("content").addChild("page1");
        page.setProperty("title", "Lucene Integration");
        root.commit();

        assertQuery("//element(*, nt:base)[@title = 'Lucene Integration']", "xpath", List.of("/content/page1"));
    }

    @Test
    public void twoCommits() throws Exception {
        // Index in first commit, content in second
        Tree def = root.getTree("/oak:index").addChild("testIdx");
        def.setProperty("jcr:primaryType", INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, List.of("title"), Type.STRINGS));
        root.commit();

        Tree page = root.getTree("/").addChild("content").addChild("page1");
        page.setProperty("title", "Lucene Integration");
        root.commit();

        assertQuery("//element(*, nt:base)[@title = 'Lucene Integration']", "xpath", List.of("/content/page1"));
    }
}
