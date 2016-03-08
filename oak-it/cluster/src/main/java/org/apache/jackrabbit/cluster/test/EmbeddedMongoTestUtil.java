package org.apache.jackrabbit.cluster.test;

import com.mongodb.MongoClient;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Created by Dominik Foerderreuther <df@adobe.com> on 29/02/16.
 */
class EmbeddedMongoTestUtil {

    private static final int port = 34567;

    private static MongodExecutable mongodExecutable;
    private static MongodProcess mongodProcess;


    public static void start() throws IOException {

        ProcessOutput processOutput = new ProcessOutput(Processors.namedConsole("[mongod>]"),
                Processors.namedConsole("[MONGOD>]"), Processors.namedConsole("[console>]"));

        ProcessOutput silentOutput = new ProcessOutput(Processors.silent(),
                Processors.silent(), Processors.silent());

        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(Command.MongoD)
                .processOutput(silentOutput)
                .build();

        MongodStarter starter = MongodStarter.getInstance(runtimeConfig);

        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()))
                .build();

        mongodExecutable = starter.prepare(mongodConfig);
        mongodProcess = mongodExecutable.start();
    }

    public static void stop() {
        mongodProcess.stop();
        mongodExecutable.stop();
    }

    public static MongoClient mongoClient() throws UnknownHostException {
        return new MongoClient("localhost", port);
    }
}
