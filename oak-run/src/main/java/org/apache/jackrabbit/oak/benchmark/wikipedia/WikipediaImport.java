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
import static java.lang.Math.min;

import java.io.File;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.benchmark.Benchmark;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.util.Text;

public class WikipediaImport extends Benchmark {

    private final File dump;

    private final boolean doReport;

    private final boolean flat;

    public WikipediaImport(File dump, boolean flat, boolean doReport) {
        this.dump = dump;
        this.flat = flat;
        this.doReport = doReport;
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
            int after = new Traversal().traverse(session);
            checkState(before == after, "Import vs. traverse mismatch");
        } finally {
            session.logout();
        }
    }

    public int importWikipedia(Session session) throws Exception {
        long start = System.currentTimeMillis();
        int count = 0;
        int code = 0;

        System.out.format("Importing %s...%n", dump);

        String type = "nt:unstructured";
        if (flat) {
            type = "oak:Unstructured";
        }
        Node wikipedia = session.getRootNode().addNode("wikipedia", type);

        int levels = 0;
        if (!flat) {
            // calculate the number of levels needed, based on the rough
            // estimate that the average XML size of a page is about 1kB
            for (long pages = dump.length() / 1024; pages > 256; pages /= 256) {
                levels++;
            }
        }

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
                    Node parent = wikipedia;
                    if (levels > 0) {
                        int n = name.length();
                        for (int i = 0; i < levels; i++) {
                            int hash = name.substring(min(i, n)).hashCode();
                            parent = JcrUtils.getOrAddNode(
                                    parent, String.format("%02x", hash & 0xff));
                        }
                    }
                    Node page = parent.addNode(name);
                    page.setProperty("title", title);
                    page.setProperty("text", text);
                    code += title.hashCode();
                    code += text.hashCode();
                    count++;
                    if (count % 1000 == 0) {
                        if (!flat) {
                            session.save();
                        }
                        if (doReport) {
                            long millis = System.currentTimeMillis() - start;
                            System.out.format(
                                    "Added %d pages in %d seconds (%.2fms/page)%n",
                                    count, millis / 1000, (double) millis / count);
                        }
                    }

                    pageAdded(title, text);
                }
                break;
            }
        }

        session.save();

        if (doReport) {
            long millis = System.currentTimeMillis() - start;
            System.out.format(
                    "Imported %d pages in %d seconds (%.2fms/page)%n",
                    count, millis / 1000, (double) millis / count);
        }

        return code;
    }

    protected void pageAdded(String title, String text) {
    }

    private class Traversal {

        private final long start = System.currentTimeMillis();
        private int count = 0;
        private int code = 0;

        private int traverse(Session session) throws Exception {
            System.out.format("Traversing imported pages...%n");
            Node wikipedia = session.getNode("/wikipedia");

            traverse(wikipedia);

            if (doReport) {
                long millis = System.currentTimeMillis() - start;
                System.out.format(
                        "Traversed %d pages in %d seconds (%.2fms/page)%n",
                        count, millis / 1000, (double) millis / count);
            }

            return code;
        }

        private void traverse(Node parent) throws RepositoryException {
            NodeIterator pages = parent.getNodes();
            while (pages.hasNext()) {
                Node page = pages.nextNode();

                code += page.getProperty("title").getString().hashCode();
                code += page.getProperty("text").getString().hashCode();

                count++;
                if (count % 1000 == 0 && doReport) {
                    long millis = System.currentTimeMillis() - start;
                    System.out.format(
                            "Read %d pages in %d seconds (%.2fms/page)%n",
                            count, millis / 1000, (double) millis / count);
                }

                traverse(page);
            }
        }

    }

}
