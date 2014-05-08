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

import static com.google.common.base.Preconditions.checkState;

import java.io.File;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
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
            for (RepositoryFixture fixture : fixtures) {
                if (fixture.isAvailable(1)) {
                    System.out.format(
                            "%s: Wikipedia import benchmark%n", fixture);
                    try {
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
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        try {
            int before = importWikipedia(session);
            int after = traverseWikipedia(session);
            checkState(before == after, "Import vs. traverse mismatch");
        } finally {
            session.logout();
        }
    }

    private int importWikipedia(Session session) throws Exception {
        long start = System.currentTimeMillis();
        int count = 0;
        int code = 0;

        System.out.format("Importing %s...%n", dump);
        Node wikipedia = session.getRootNode().addNode(
                "wikipedia", "oak:Unstructured");

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
                    code += title.hashCode();
                    code += text.hashCode();
                    count++;
                    if (count % 1000 == 0) {
                        long millis = System.currentTimeMillis() - start;
                        System.out.format(
                                "Added %d pages in %d seconds (%.2fms/page)%n",
                                count, millis / 1000, (double) millis / count);
                    }
                }
                break;
            }
        }

        session.save();

        long millis = System.currentTimeMillis() - start;
        System.out.format(
                "Imported %d pages in %d seconds (%.2fms/page)%n",
                count, millis / 1000, (double) millis / count);
        return code;
    }

    private int traverseWikipedia(Session session) throws Exception {
        long start = System.currentTimeMillis();
        int count = 0;
        int code = 0;

        System.out.format("Traversing imported pages...%n");
        Node wikipedia = session.getNode("/wikipedia");

        NodeIterator pages = wikipedia.getNodes();
        while (pages.hasNext()) {
            Node page = pages.nextNode();
            code += page.getProperty("title").getString().hashCode();
            code += page.getProperty("text").getString().hashCode();
            count++;
            if (count % 1000 == 0) {
                long millis = System.currentTimeMillis() - start;
                System.out.format(
                        "Read %d pages in %d seconds (%.2fms/page)%n",
                        count, millis / 1000, (double) millis / count);
            }
        }

        long millis = System.currentTimeMillis() - start;
        System.out.format(
                "Traversed %d pages in %d seconds (%.2fms/page)%n",
                count, millis / 1000, (double) millis / count);
        return code;
    }

}
