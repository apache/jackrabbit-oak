package org.apache.jackrabbit.cluster.test;

import com.google.common.base.Preconditions;
import com.mongodb.MongoClient;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;

import javax.jcr.Repository;
import java.net.UnknownHostException;

import static org.apache.jackrabbit.cluster.test.EmbeddedMongoTestUtil.mongoClient;
import static org.apache.jackrabbit.cluster.test.MongoTestUtil.databaseExist;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 02/03/16.
 */
public class OakClusterRepository extends ExternalResource {

    private static final Logger log = Logger.getLogger(OakClusterRepository.class);

    private static final String DBNAME = "testrepo";

    @Override
    protected void before() throws Throwable {
        EmbeddedMongoTestUtil.start();

        MongoClient clientOne = mongoClient();

        MongoTestUtil.dropDatabase(clientOne, DBNAME);
        Preconditions.checkArgument(!databaseExist(clientOne, DBNAME));
    }

    public Repository repository(int asyncDelay, int maxBackOffMillis) throws OakClusterRepositoryException {
        Repository repository;
        try {
            repository = OakTestUtil.connect(mongoClient(), DBNAME, asyncDelay, maxBackOffMillis);
        } catch (UnknownHostException e) {
            throw new OakClusterRepositoryException(e);
        }
        sleep();
        return repository;
    }

    public Repository repository() throws OakClusterRepositoryException {
        Repository repository;
        try {
            repository = OakTestUtil.connect(mongoClient(), DBNAME);
        } catch (UnknownHostException e) {
            throw new OakClusterRepositoryException(e);
        }
        sleep();
        return repository;
    }

    @Override
    protected void after() {
        EmbeddedMongoTestUtil.stop();
    }

    public class OakClusterRepositoryException extends Exception {
        public OakClusterRepositoryException(Throwable cause) {
            super(cause);
        }
    }

    private void sleep() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

}
