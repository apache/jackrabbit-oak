package org.apache.jackrabbit.cluster.test;

import com.mongodb.MongoClient;

import java.net.UnknownHostException;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
class MongoTestUtil {

    public static void dropDatabase(MongoClient mongoClient, String dbname) {
        mongoClient.dropDatabase(dbname);
    }

    public static MongoClient mongoClient(String host) throws UnknownHostException {
        return new MongoClient(host);
    }

    public static boolean databaseExist(MongoClient mongoClient, String dbname) {
        return mongoClient.getDatabaseNames().contains(dbname);
    }
}
