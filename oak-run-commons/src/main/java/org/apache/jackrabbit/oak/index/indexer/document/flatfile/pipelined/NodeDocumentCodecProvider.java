package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

public class NodeDocumentCodecProvider implements CodecProvider {
    private final MongoDocumentStore store;
    private final Collection<NodeDocument> col;

    public NodeDocumentCodecProvider(MongoDocumentStore store, Collection<NodeDocument> col) {
        this.store = store;
        this.col = col;
    }

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (clazz == NodeDocument.class) {
            NodeDocumentCodec nodeDocumentCodec = new NodeDocumentCodec(store, col, registry);
            return (Codec<T>) nodeDocumentCodec;
        }
        return null;
    }
}
