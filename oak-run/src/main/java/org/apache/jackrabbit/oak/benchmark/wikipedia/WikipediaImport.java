/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.benchmark.wikipedia;

import java.io.File;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import org.apache.jackrabbit.oak.benchmark.Benchmark;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.util.Text;

public class WikipediaImport extends Benchmark {

    private final File dump;

    public WikipediaImport(File dump) {
        this.dump = dump;
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        if (dump.isFile()) {
            System.out.format("Wikipedia import (%s)%n", dump);
            for (RepositoryFixture fixture : fixtures) {
                if (fixture.isAvailable(1)) {
                    try {
                        System.out.format("%s: importing Wikipedia...%n", fixture);
                        Repository[] cluster = fixture.setUpCluster(1);
                        try {
                            run(cluster[0]);
                        } finally {
                            fixture.tearDownCluster();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.format("%s: not available, skipping.%n", fixture);
                }
            }
        } else {
            System.out.format(
                    "Missing Wikipedia dump %s, skipping import benchmark.%n",
                    dump.getPath());
        }
    }

    private void run(Repository repository) throws Exception {
        long start = System.currentTimeMillis();
        int pages = 0;

        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        try {
            Node wikipedia = session.getRootNode().addNode("wikipedia");

            String title = null;
            String text = null;
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader reader =
                    factory.createXMLStreamReader(new StreamSource(dump));
            while (reader.hasNext()) {
                switch (reader.next()) {
                case XMLStreamConstants.START_ELEMENT:
                    if ("title".equals(reader.getLocalName())) {
                        title = reader.getElementText();
                    } else if ("text".equals(reader.getLocalName())) {
                        text = reader.getElementText();
                    }
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    if ("page".equals(reader.getLocalName())) {
                        String name = Text.escapeIllegalJcrChars(title);
                        Node page = wikipedia.addNode(name);
                        page.setProperty("title", title);
                        page.setProperty("text", text);
                        pages++;
                        if (pages % 1000 == 0) {
                            long millis = System.currentTimeMillis() - start;
                            System.out.format(
                                    "Imported %d pages in %d seconds (%dus/page)%n",
                                    pages, millis / 1000, millis * 1000 / pages);
                        }

                    }
                    break;
                }
            }

            session.save();
        } finally {
            session.logout();
        }

        long millis = System.currentTimeMillis() - start;
        System.out.format(
                "Imported %d pages in %d seconds (%dus/page)%n",
                pages, millis / 1000, millis * 1000 / pages);
    }

}
