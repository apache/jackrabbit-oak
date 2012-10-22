package org.apache.jackrabbit.mongomk.command;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.junit.Test;

public class ConcurrentWriteMultipleMkMongoTest extends BaseMongoTest {

    @Test
    public void testConcurrency() throws NumberFormatException, Exception {
        
        String diff1 = buildPyramidDiff("/", 0, 3, 10, "N", new StringBuilder())
                .toString();
        String diff2 = buildPyramidDiff("/", 0, 3, 10, "P", new StringBuilder())
                .toString();
        String diff3 = buildPyramidDiff("/", 0, 3, 10, "R", new StringBuilder())
                .toString();

        System.out.println(diff1);
        System.out.println(diff2);
        System.out.println(diff3);

        InputStream is = BaseMongoTest.class.getResourceAsStream("/config.cfg");
        Properties properties = new Properties();
        properties.load(is);

        String host = properties.getProperty("host");
        int port = Integer.parseInt(properties.getProperty("port"));
        String db = properties.getProperty("db");

        GenericWriteTask task1 = new GenericWriteTask(diff1, 0,
                new MongoConnection(host, port, db));
        GenericWriteTask task2 = new GenericWriteTask(diff2, 0,
                new MongoConnection(host, port, db));
        GenericWriteTask task3 = new GenericWriteTask(diff3, 0,
                new MongoConnection(host, port, db));

        ExecutorService threadExecutor = Executors.newFixedThreadPool(3);
        threadExecutor.execute(task1);
        threadExecutor.execute(task2);
        threadExecutor.execute(task3);
        threadExecutor.shutdown();
        threadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    private static StringBuilder buildPyramidDiff(String startingPoint,
            int index, int numberOfChildren, long nodesNumber,
            String nodePrefixName, StringBuilder diff) {
        if (numberOfChildren == 0) {
            for (long i = 0; i < nodesNumber; i++)
                diff.append(addNodeToDiff(startingPoint, nodePrefixName + i));
            return diff;
        }
        if (index >= nodesNumber)
            return diff;
        diff.append(addNodeToDiff(startingPoint, nodePrefixName + index));
        // System.out.println("Create node "+ index);
        for (int i = 1; i <= numberOfChildren; i++) {
            if (!startingPoint.endsWith("/"))
                startingPoint = startingPoint + "/";
            buildPyramidDiff(startingPoint + nodePrefixName + index, index
                    * numberOfChildren + i, numberOfChildren, nodesNumber,
                    nodePrefixName, diff);
        }
        return diff;
    }

    private static String addNodeToDiff(String startingPoint, String nodeName) {
        if (!startingPoint.endsWith("/"))
            startingPoint = startingPoint + "/";

        return ("+\"" + startingPoint + nodeName + "\" : {\"key\":\"00000000000000000000\"} \n");
    }

}

class GenericWriteTask implements Runnable {

    MongoConnection mongoConnection;
    String diff;
    int nodesPerCommit;

    public GenericWriteTask(String diff, int nodesPerCommit,
            MongoConnection mongoConnection) {

        this.diff = diff;
        this.mongoConnection = mongoConnection;
    }

    @Override
    public void run() {

        Commit commit;
        try {
            commit = CommitBuilder.build("", diff, "Add message");
            CommitCommandMongo command = new CommitCommandMongo(
                    mongoConnection, commit);
            command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
