package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.junit.Test;

import static org.junit.Assert.*;

public class MongoVersionTest {


    @Test
    public void createMongoVersion() {
        MongoVersion version = MongoVersion.of("4.0.0");
        assertEquals(MongoVersion.MONGO_4_0_0, version);
    }

    @Test
    public void createDefaultMongoVersionFromNull() {
        MongoVersion version = MongoVersion.of(null);
        assertEquals(MongoVersion.MONGO_4_0_0, version);
    }

    @Test
    public void createDefaultMongoVersionMalformed() {
        MongoVersion version = MongoVersion.of("234");
        assertEquals(MongoVersion.MONGO_4_0_0, version);
    }

    @Test
    public void createDefaultMongoVersionMalformed2() {
        MongoVersion version = MongoVersion.of("4.0");
        assertEquals(MongoVersion.MONGO_4_0_0, version);
    }

    @Test
    public void compareMongoVersion() {
        MongoVersion version = MongoVersion.of("2.3.4");
        assertTrue(MongoVersion.MONGO_4_0_0.compareTo(version) > 0);
    }

    @Test
    public void compareMongoVersion2() {
        MongoVersion version = MongoVersion.of("4.0.4");
        assertTrue(MongoVersion.MONGO_4_0_0.compareTo(version) < 0);
    }

}