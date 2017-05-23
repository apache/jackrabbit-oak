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
package org.apache.jackrabbit.oak.run;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import com.google.common.util.concurrent.MoreExecutors;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.DocumentQueue;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

public class JsonIndexCommand implements Command {
    public static final String INDEX = "json-index";

    PrintStream output = System.out;
    Session session;
    private boolean interactive;
    private final Map<String, Object> data = new HashMap<String, Object>();

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> scriptOption = parser
                .accepts("script", "Path to Script").withOptionalArg()
                .defaultsTo("");

        Options oakOptions = new Options();
        OptionSet options = oakOptions.parseAndConfigure(parser, args);

        System.out.println("Opening nodestore...");
        NodeStoreFixture nodeStoreFixture = NodeStoreFixtureProvider.create(oakOptions);

        NodeStore nodeStore = nodeStoreFixture.getStore();
        String script = scriptOption.value(options);
        LineNumberReader reader = openScriptReader(script);
        try {
            process(nodeStore, reader);
        } finally {
            nodeStoreFixture.close();
            reader.close();
        }
    }

    private LineNumberReader openScriptReader(String script)
            throws IOException {
        Reader reader;
        if ("-".equals(script)) {
            reader = new InputStreamReader(System.in);
            interactive = true;
        } else {
            reader = new FileReader(script);
        }
        return new LineNumberReader(new BufferedReader(reader));
    }

    public void process(NodeStore nodeStore, LineNumberReader reader)
            throws Exception {
        session = openSession(nodeStore);
        System.out.println("Nodestore is open");
        if (interactive) {
            System.out.println("Type \"exit\" to quit");
        }
        while (true) {
            try {
                String json = readJson(reader);
                if (json == null || json.trim().equals("exit")) {
                    break;
                }
                execute(json);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (session != null) {
            session.logout();
        }
    }

    private static String readJson(LineNumberReader reader) throws IOException {
        StringBuilder buff = new StringBuilder();
        int level = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                return null;
            } else if (line.trim().startsWith("//")) {
                continue;
            }
            buff.append(line).append('\n');
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '\"') {
                    while (true) {
                        c = line.charAt(++i);
                        if (c == '\"') {
                            break;
                        } else if (c == '\\') {
                            ++i;
                        }
                    }
                } else if (c == '{') {
                    level++;
                } else if (c == '}') {
                    level--;
                }
            }
            if (level == 0 && !buff.toString().trim().isEmpty()) {
                return buff.toString();
            }
        }
    }
    
    void execute(String command) throws RepositoryException {
        JsopTokenizer t = new JsopTokenizer(command);
        t.read('{');
        JsonObject json = JsonObject.create(t);
        Map<String, String> properties = json.getProperties();
        if (properties.containsKey("if")) {
            Object value = getValueOrVariable(properties.get("if"));
            Object equals = getValueOrVariable(properties.get("="));
            if (value == null) {
                if (equals != null) {
                    return;
                }
            } else if (!value.equals(equals)) {
                return;
            }
        }
        for (Entry<String, String> e : properties.entrySet()) {
            String k = e.getKey();
            Object value = getValueOrVariable(e.getValue());
            if ("addNode".equals(k)) {
                String nodePath = value.toString();
                String parent = PathUtils.getParentPath(nodePath);
                if (session.nodeExists(parent)) {
                    Node p = session.getNode(parent);
                    String nodeName = PathUtils.getName(nodePath);
                    if (!p.hasNode(nodeName)) {
                        JsonObject node = json.getChildren().get("node");
                        addNode(p, nodeName, node);
                    }
                }
            } else if ("removeNode".equals(k)) {
                String path = value.toString();
                if (session.nodeExists(path)) {
                    session.getNode(path).remove();
                }
            } else if ("setProperty".equals(k)) {
                String itemPath = value.toString();
                String nodePath = PathUtils.getParentPath(itemPath);
                if (session.nodeExists(nodePath)) {
                    String propertyName = PathUtils.getName(itemPath);
                    Object propertyValue = getValueOrVariable(properties
                            .get("value"));
                    setProperty(session.getNode(nodePath), propertyName,
                            propertyValue);
                }
            } else if ("session".equals(k)) {
                if ("save".equals(value)) {
                    session.save();
                }
            } else if ("xpath".equals(k) || "sql".equals(k)) {
                String language = "xpath".equals(k) ? k : Query.JCR_SQL2;
                String columnName = "xpath".equals(k) ? "jcr:path" : null;
                boolean quiet = properties.containsKey("quiet");
                int depth = properties.containsKey("depth") ? Integer
                        .parseInt(properties.get("depth")) : 0;
                runQuery(value.toString(), language, columnName, quiet, depth);
            } else if ("print".equals(k)) {
                output.println(value);
            } else if ("for".equals(k)) {
                String name = JsopTokenizer.decodeQuoted(properties.get(k));
                Object old = data.get(name);
                String[] commands = (String[]) getValueOrVariable(properties
                        .get("do"));
                for (String x : (String[]) value) {
                    data.put(name, x);
                    for (String c : commands) {
                        execute(c);
                    }
                }
                data.put(name, old);
            } else if ("loop".equals(k)) {
                while (true) {
                    for (String c : (String[]) value) {
                        execute(c);
                        if (data.remove("$break") != null) {
                            return;
                        }
                    }
                }
            } else if (k.startsWith("$")) {
                setVariable(properties, k, value);
            }
        }
    }

    private void setVariable(Map<String, String> properties, String k,
            Object value) {
        if (k.startsWith("$$")) {
            k = "$" + getValueOrVariable("\"" + k.substring(1) + "\"");
        }
        if (properties.containsKey("+")) {
            Object v2 = getValueOrVariable(properties.get("+"));
            if (value == null) {
                value = v2;
            } else if (v2 == null) {
                // keep value
            } else if (v2 instanceof Long && value instanceof Long) {
                value = (Long) value + (Long) v2;
            } else {
                value = value.toString() + v2.toString();
            }
        }
        data.put(k, value);
    }

    private Object getValueOrVariable(String jsonValue) {
        Object v = getValue(jsonValue);
        if (v == null || !v.toString().startsWith("$")) {
            return v;
        }
        String value = v.toString();
        if (value.startsWith("$$")) {
            value = "$" + getValueOrVariable("\"" + value.substring(1) + "\"");
        }
        return data.get(value.toString());
    }

    private void addNode(Node p, String nodeName, JsonObject json)
            throws RepositoryException {
        Map<String, String> properties = json.getProperties();
        Map<String, JsonObject> children = json.getChildren();
        String primaryType = properties.get("jcr:primaryType");
        Node n;
        if (primaryType == null) {
            n = p.addNode(nodeName);
        } else {
            n = p.addNode(nodeName, getValueOrVariable(primaryType).toString());
        }
        for (Entry<String, String> e : properties.entrySet()) {
            String propertyName = e.getKey();
            if (!"jcr:primaryType".equals(propertyName)) {
                Object value = getValueOrVariable(properties.get(propertyName));
                setProperty(n, propertyName, value);
            }
        }
        for (Entry<String, JsonObject> e : children.entrySet()) {
            String k = e.getKey();
            JsonObject v = e.getValue();
            addNode(n, k, v);
        }
    }

    private static Object getValue(String jsonValue) {
        if (jsonValue == null) {
            return null;
        }
        JsopTokenizer t = new JsopTokenizer(jsonValue);
        if (t.matches(JsopReader.NULL)) {
            return null;
        } else if (t.matches(JsopReader.NUMBER)) {
            String n = t.getToken();
            if (n.indexOf('.') < 0) {
                return Long.parseLong(n);
            }
            return Double.parseDouble(n);
        } else if (t.matches(JsopReader.TRUE)) {
            return true;
        } else if (t.matches(JsopReader.FALSE)) {
            return false;
        } else if (t.matches(JsopReader.STRING)) {
            return t.getToken();
        } else if (t.matches('[')) {
            ArrayList<String> list = new ArrayList<String>();
            if (!t.matches(']')) {
                while (true) {
                    list.add(t.readRawValue());
                    if (t.matches(']')) {
                        break;
                    }
                    t.read(',');
                }
            }
            return list.toArray(new String[0]);
        }
        throw new IllegalArgumentException(jsonValue);
    }

    private static void setProperty(Node n, String propertyName, Object value)
            throws RepositoryException {
        int type = PropertyType.UNDEFINED;
        if (propertyName.startsWith("{")) {
            String t = propertyName.substring(1, propertyName.indexOf('}'));
            propertyName = propertyName.substring(t.length() + 2);
            type = PropertyType.valueFromName(t);
        }
        if (value == null) {
            n.setProperty(propertyName, (String) null);
            return;
        }
        if (type == PropertyType.UNDEFINED) {
            if (value instanceof Boolean) {
                type = PropertyType.BOOLEAN;
            } else if (value instanceof Long) {
                type = PropertyType.LONG;
            } else if (value instanceof Double) {
                type = PropertyType.DOUBLE;
            } else {
                type = PropertyType.STRING;
            }
        }
        if (value instanceof String[]) {
            String[] list = (String[]) value;
            for (int i = 0; i < list.length; i++) {
                list[i] = getValue(list[i]).toString();
            }
            n.setProperty(propertyName, list, type);
        } else {
            n.setProperty(propertyName, value.toString(), type);
        }
    }

    private void runQuery(String query, String language, String columnName,
            boolean quiet, int depth) throws RepositoryException {
        ArrayList<String> list = new ArrayList<String>();
        columnName = query.startsWith("explain") ? "plan" : columnName;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query q = qm.createQuery(query, language);
        for (String b : q.getBindVariableNames()) {
            ValueFactory vf = session.getValueFactory();
            q.bindValue(b, vf.createValue(data.get("$" + b).toString()));
        }
        QueryResult result = q.execute();
        if (depth != 0) {
            NodeIterator ni = result.getNodes();
            JsopBuilder builder = new JsopBuilder().array();
            while (ni.hasNext()) {
                Node n = ni.nextNode();
                builder.key(n.getPath());
                appendNode(builder, n, depth - 1);
            }
            output.println(JsopBuilder.prettyPrint(builder.endArray().toString()));
            return;
        }
        RowIterator ri = result.getRows();
        while (ri.hasNext()) {
            Row r = ri.nextRow();
            if (columnName != null) {
                String x = r.getValue(columnName).getString();
                list.add(x);
                if (!quiet) {
                    output.println(x);
                }
            } else {
                String[] columnNames = result.getColumnNames();
                for (String cn : columnNames) {
                    Value v = r.getValue(cn);
                    String x = v == null ? null : v.getString();
                    if (columnNames.length == 1) {
                        list.add(x);
                        if (!quiet) {
                            output.println(x);
                        }
                    } else {
                        list.add(x);
                        if (!quiet) {
                            output.println(cn + ": " + x);
                        }
                    }
                }
            }
        }
        data.put("$resultSize", (long) list.size());
        data.put("$result", list.toArray(new String[0]));
    }

    private void appendNode(JsopBuilder builder, Node n, int depth)
            throws RepositoryException {
        builder.object();
        for (PropertyIterator it = n.getProperties(); depth != 0 &&
                it.hasNext();) {
            Property p = it.nextProperty();
            String name = (p.getType() == PropertyType.STRING ||
                    p.getName().equals("jcr:primaryType") ? "" : "{" +
                    PropertyType.nameFromValue(p.getType()) + "}") +
                    p.getName();
            builder.key(name);
            if (p.isMultiple()) {
                builder.array();
                for (Value v : p.getValues()) {
                    builder.value(v.getString());
                }
                builder.endArray();
            } else {
                builder.value(p.getValue().getString());
            }
        }
        for (NodeIterator it = n.getNodes(); depth != 0 && it.hasNext();) {
            Node n2 = it.nextNode();
            builder.key(n2.getName());
            appendNode(builder, n2, depth - 1);
        }
        builder.endObject();
    }

    public static Session openSession(NodeStore nodeStore) throws RepositoryException {
        if (nodeStore == null) {
            return null;
        }
        StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
        Oak oak = new Oak(nodeStore).with(ManagementFactory.getPlatformMBeanServer());
        oak.getWhiteboard().register(StatisticsProvider.class, statisticsProvider, Collections.emptyMap());
        LuceneIndexProvider provider = createLuceneIndexProvider();
        oak.with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(createLuceneIndexEditorProvider());
        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    private static LuceneIndexEditorProvider createLuceneIndexEditorProvider() {
        LuceneIndexEditorProvider ep = new LuceneIndexEditorProvider();
        ScheduledExecutorService executorService = MoreExecutors.getExitingScheduledExecutorService(
                (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(5));
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        int queueSize = Integer.getInteger("queueSize", 1000);
        IndexTracker tracker = new IndexTracker();
        DocumentQueue queue = new DocumentQueue(queueSize, tracker, executorService, statsProvider);
        ep.setIndexingQueue(queue);
        return ep;
    }

    private static LuceneIndexProvider createLuceneIndexProvider() {
        return new LuceneIndexProvider();
    }

}
