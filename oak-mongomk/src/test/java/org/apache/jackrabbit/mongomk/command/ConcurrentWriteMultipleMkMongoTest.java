package org.apache.jackrabbit.mongomk.command;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.impl.BlobStoreMongo;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.NodeStoreMongo;

import org.junit.Test;

public class ConcurrentWriteMultipleMkMongoTest extends BaseMongoTest {

    @Test
    public void testConcurrency() throws NumberFormatException, Exception {

        String diff1 = buildPyramidDiff("/", 0, 10, 100, "N",
                new StringBuilder()).toString();
        String diff2 = buildPyramidDiff("/", 0, 10, 100, "P",
                new StringBuilder()).toString();
        String diff3 = buildPyramidDiff("/", 0, 10, 100, "R",
                new StringBuilder()).toString();

        // System.out.println(diff1);
        // System.out.println(diff2);
        // System.out.println(diff3);

        InputStream is = BaseMongoTest.class.getResourceAsStream("/config.cfg");
        Properties properties = new Properties();
        properties.load(is);

        String host = properties.getProperty("host");
        int port = Integer.parseInt(properties.getProperty("port"));
        String db = properties.getProperty("db");

        MongoMicroKernel mongo1 = new MongoMicroKernel(new NodeStoreMongo(
                mongoConnection), new BlobStoreMongo(mongoConnection));
        MongoMicroKernel mongo2 = new MongoMicroKernel(new NodeStoreMongo(
                mongoConnection), new BlobStoreMongo(mongoConnection));
        MongoMicroKernel mongo3 = new MongoMicroKernel(new NodeStoreMongo(
                mongoConnection), new BlobStoreMongo(mongoConnection));

        GenericWriteTask task1 = new GenericWriteTask(mongo1, diff1, 0,
                new MongoConnection(host, port, db));
        GenericWriteTask task2 = new GenericWriteTask(mongo2, diff2, 0,
                new MongoConnection(host, port, db));
        GenericWriteTask task3 = new GenericWriteTask(mongo3, diff3, 0,
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

    MicroKernel mk;
    String diff;
    int nodesPerCommit;

    public GenericWriteTask(MongoMicroKernel mk, String diff,
            int nodesPerCommit, MongoConnection mongoConnection) {

        this.diff = diff;
        this.mk = mk;
    }

    @Override
    public void run() {
        commit(mk, diff, 10);
    }
    
    private void commit(MicroKernel mk, String diff, int nodesPerCommit) {

        if (nodesPerCommit == 0) {
            mk.commit("", diff.toString(), null, "");
            return;
        }
        String[] string = diff.split(System.getProperty("line.separator"));
        int i = 0;
        StringBuilder finalCommit = new StringBuilder();
        for (String line : string) {
            finalCommit.append(line);
            i++;
            if (i == nodesPerCommit) {
                mk.commit("", finalCommit.toString(), null, "");
                finalCommit.setLength(0);
                i = 0;
            }
        }
        // commit remaining nodes
        if (finalCommit.length() > 0)
            mk.commit("", finalCommit.toString(), null, "");
    }
}
