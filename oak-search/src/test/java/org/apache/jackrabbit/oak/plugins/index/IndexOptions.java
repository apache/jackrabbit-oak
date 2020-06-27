package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public abstract class IndexOptions {

    public abstract String getIndexType();

    protected Tree setIndex(Root root, String idxName, IndexDefinitionBuilder builder) {
        return builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    protected Node setIndex(Session session, String idxName, IndexDefinitionBuilder builder) throws RepositoryException {
        return builder.build(session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).addNode(idxName));
    }

    protected Node getIndexNode(Session session, String idxName) throws RepositoryException {
        return session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode(idxName);
    }

    protected IndexDefinitionBuilder createIndex(IndexDefinitionBuilder builder, boolean isAsync, String... propNames) {
        if (!isAsync) {
            builder = builder.noAsync();
        }
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        for (String propName : propNames) {
            indexRule.property(propName).propertyIndex();
        }
        return builder;
    }

    protected abstract IndexDefinitionBuilder createIndexDefinitionBuilder();

}
