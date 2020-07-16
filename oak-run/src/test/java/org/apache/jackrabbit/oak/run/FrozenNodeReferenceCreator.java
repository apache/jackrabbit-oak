/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.run;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Session;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.io.Closer;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Test class that simplifies creation of a property of type Reference, as that's not
 * easily achievable without a tool.
 * <p/>
 * The idea is that this might help testing the corresponding FrozenNodeRefsByScanningCommand
 * and FrozenNodeRefsUsingIndexCommand commands.
 * <p/>
 * Example:
 * <pre>
 * java -mx4g -cp oak-run-*-tests.jar org.apache.jackrabbit.oak.run.FrozenNodeReferenceCreator mongodb://localhost/&lt;dbname&gt; -user=admin -password=admin -testCreateRefPath=&lt;mypath&gt; -testCreateRefProp=&lt;mypropertyname&gt; -testCreateRefUuid=&lt;myuuid&gt; -read-write=true
 * </pre>
 */
public class FrozenNodeReferenceCreator {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();

        Options opts = new Options();
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);

        OptionSpec<String> userOption = parser.accepts("user", "User name").withOptionalArg().defaultsTo("admin");
        OptionSpec<String> passwordOption = parser.accepts("password", "Password").withOptionalArg().defaultsTo("admin");

        OptionSpec<String> testCreateRefPathOption = parser.accepts("testCreateRefPath", "FOR TESTING ONLY: path where to create a reference from")
                .withRequiredArg();
        OptionSpec<String> testCreateRefPropOption = parser.accepts("testCreateRefProp", "FOR TESTING ONLY: property name for create a reference")
                .withRequiredArg();
        OptionSpec<String> testCreateRefTypeOption = parser
                .accepts("testCreateRefType", "FOR TESTING ONLY: property type: 'reference' or anything else for a plain String").withOptionalArg();
        OptionSpec<String> testCreateRefUuidOption = parser.accepts("testCreateRefUuid", "FOR TESTING ONLY: uuid to use as the reference")
                .withRequiredArg();

        OptionSet options = opts.parseAndConfigure(parser, args);

        System.out.println("Opening nodestore...");
        NodeStoreFixture nodeStoreFixture = NodeStoreFixtureProvider.create(opts);
        System.out.println("Nodestore opened.");

        NodeStore nodeStore = nodeStoreFixture.getStore();
        String user = userOption.value(options);
        String password = passwordOption.value(options);

        String createRefPath = testCreateRefPathOption.value(options);
        String createRefProp = testCreateRefPropOption.value(options);
        String createRefType = testCreateRefTypeOption.value(options);
        String createRefUuid = testCreateRefUuidOption.value(options);

        Closer closer = Utils.createCloserWithShutdownHook();
        closer.register(nodeStoreFixture);
        try {

            System.out.println("Logging in...");
            Session session = FrozenNodeRefsByScanningCommand.openSession(nodeStore, "crx.default", user, password);

            System.out.println(
                    "Logged in, creating test reference: " + "path=" + createRefPath + ", property=" + createRefProp + ", uuid=" + createRefUuid);
            Node n = session.getNode(createRefPath);
            Value v;
            if ("reference".equals(createRefType)) {
                v = session.getValueFactory().createValue(createRefUuid, PropertyType.REFERENCE);
                System.out.println("Creating property of type REFERENCE");
            } else {
                v = session.getValueFactory().createValue(createRefUuid, PropertyType.STRING);
                System.out.println("Creating property of type STRING");
            }
            n.setProperty(createRefProp, v);
            session.save();
            System.out.println("Created. Done.");
            session.logout();
            closer.close();

        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
}
