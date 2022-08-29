package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * defines default mongodb config, provides support to add system properties
 */
public class MongoDBConfig {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBConfig.class);

    private static final String STORAGE_ENGINE = "wiredTiger";
    private static final String CONFIG = "configString";
    private static final String ZSTD_COMPRESSION = "\"block_compressor=zstd\"";

    public static Bson getCollectionStorageOptions(){
        JsonObject root = new JsonObject();
        JsonObject configString = new JsonObject();
        configString.getProperties().put(CONFIG, ZSTD_COMPRESSION);
            root.getChildren().put(STORAGE_ENGINE, configString);

        LOG.info("Collection Config:" + root.toString());
        Bson storageOptions = BsonDocument.parse(root.toString());
        return storageOptions;
    }

}
