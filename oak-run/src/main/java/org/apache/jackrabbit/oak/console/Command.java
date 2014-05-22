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
package org.apache.jackrabbit.oak.console;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

/**
 * All available console commands.
 */
public abstract class Command implements Callable<Object> {

    private static final char[] SPACES = new char[4];

    static {
        Arrays.fill(SPACES, ' ');
    }

    protected String args;
    protected String description;
    protected String usage;
    protected ConsoleSession session;
    protected InputStream in;
    protected OutputStream out;

    @Nonnull
    public static List<Command> create(String line) throws IOException {
        List<Command> commands = Lists.newArrayList();
        for (String s : line.split("\\|")) {
            commands.add(parse(s));
        }
        return commands;
    }

    @Nonnull
    private static Command parse(String line) throws IOException {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (; i < line.length(); i++) {
            char c = line.charAt(i);
            if (Character.isWhitespace(c)) {
                if (sb.length() == 0) {
                    continue;
                } else {
                    break;
                }
            }
            if (sb.length() == 0) {
                c = Character.toUpperCase(c);
            } else {
                c = Character.toLowerCase(c);
            }
            sb.append(c);
        }

        if (sb.length() == 0) {
            return new NoOp();
        }

        Command cmd;
        try {
            cmd = Class.forName(Command.class.getName() + "$" + sb.toString())
                    .asSubclass(Command.class).newInstance();
        } catch (Exception e) {
            cmd = new Unknown(sb.toString().toLowerCase(Locale.US));
        }
        cmd.setArgs(line.substring(i).trim());
        return cmd;
    }

    protected final void setArgs(String args) {
        this.args = args;
    }

    protected final void init(@Nonnull ConsoleSession session,
                              @Nonnull InputStream in,
                              @Nonnull OutputStream out) {
        this.session = session;
        this.in = in;
        this.out = out;
    }

    @Override
    public Object call() throws Exception {
        execute();
        in.close();
        out.close();
        return null;
    }

    public abstract void execute() throws Exception;

    public String getName() {
        return getClass().getSimpleName().toLowerCase(Locale.US);
    }

    protected void println(Object value, OutputStream out) throws IOException {
        Writer writer = new OutputStreamWriter(out);
        if (value == null) {
            writer.write("[null]");
        } else {
            writer.write(value.toString());
        }
        println(writer);
        writer.flush();
    }

    protected static void println(Writer writer) throws IOException {
        writer.write(StandardSystemProperty.LINE_SEPARATOR.value());
    }

    private static final class Unknown extends Command {

        private final String cmd;

        private Unknown(String cmd) {
            this.cmd = cmd;
        }

        @Override
        public void execute() throws IOException {
            println("Unknown command: " + cmd + " " + args, out);
        }
    }

    private static final class NoOp extends Command {

        @Override
        public void execute() throws IOException {
        }
    }

    public static final class Help extends Command {

        public Help() {
            this.description = "List all available command.";
        }

        @Override
        public void execute() throws Exception {
            SortedMap<String, Command> commands = Maps.newTreeMap();
            SortedMap<String, Command> docCommands = Maps.newTreeMap();
            Class[] classes = Command.class.getDeclaredClasses();
            int maxCmdLength = 0;
            for (Class clazz : classes) {
                if (!Modifier.isPublic(clazz.getModifiers())
                        || !Command.class.isAssignableFrom(clazz)) {
                    continue;
                }
                Command cmd = (Command) clazz.newInstance();
                if (DocumentNodeStoreCommand.class.isAssignableFrom(clazz)) {
                    docCommands.put(cmd.getName(), cmd);
                } else {
                    commands.put(cmd.getName(), cmd);
                }
                maxCmdLength = Math.max(maxCmdLength, cmd.getName().length());
            }
            println("Generic commands:", out);
            Writer writer = new OutputStreamWriter(out);
            printCommands(commands, maxCmdLength, writer);
            println(writer);
            writer.flush();
            println("DocumentNodeStore specific commands:", out);
            printCommands(docCommands, maxCmdLength, writer);
            writer.flush();
        }

        private void printCommands(SortedMap<String, Command> commands,
                                   int maxCmdLength,
                                   Writer writer) throws IOException {
            for (String c : commands.keySet()) {
                Command cmd = commands.get(c);
                writer.write(SPACES);
                writer.write(c);
                int numSpaces = maxCmdLength - c.length() + 4;
                for (int i = 0; i < numSpaces; i++) {
                    writer.write(" ");
                }
                if (cmd.description != null) {
                    writer.write(cmd.description);
                }
                println(writer);
            }
        }
    }

    public static final class Pwd extends Command {

        public Pwd() {
            this.description = "Print the full path of the current working node.";
        }

        @Override
        public void execute() throws IOException {
            println(session.getWorkingPath(), out);
        }
    }

    public static final class Exit extends Command {

        public Exit() {
            this.description = "Quit this console.";
        }

        @Override
        public void execute() throws IOException {
            println("Good bye.", out);
        }
    }

    public static final class Cd extends Command {

        public Cd() {
            this.description = "Change the current working directory to a specific node.";
        }

        @Override
        public void execute() throws IOException {
            if (!PathUtils.isValid(args)) {
                println("Not a valid path: " + args, out);
                return;
            }
            String path;
            if (PathUtils.isAbsolute(args)) {
                path = args;
            } else {
                path = PathUtils.concat(session.getWorkingPath(), args);
            }
            List<String> elements = Lists.newArrayList();
            for (String element : PathUtils.elements(path)) {
                if (PathUtils.denotesParent(element)) {
                    if (!elements.isEmpty()) {
                        elements.remove(elements.size() - 1);
                    }
                } else if (!PathUtils.denotesCurrent(element)) {
                    elements.add(element);
                }
            }
            path = PathUtils.concat("/", elements.toArray(new String[elements.size()]));
            String old = session.setWorkingPath(path);
            if (!session.getWorkingNode().exists()) {
                session.setWorkingPath(old);
                println("No such node", out);
            }
        }
    }

    public static final class Ls extends Command {

        public Ls() {
            this.description = "List the names of the children of the current node.";
        }

        @Override
        public void execute() throws IOException {
            for (String name : session.getWorkingNode().getChildNodeNames()) {
                println(name, out);
            }
        }
    }

    public static final class Pn extends Command {

        public Pn() {
            this.description = "Print the current node.";
        }

        @Override
        public void execute() throws IOException {
            println(AbstractNodeState.toString(session.getWorkingNode()), out);
        }
    }

    public static final class Refresh extends Command {

        public Refresh() {
            this.description = "Control how the current session is refreshed.";
        }

        @Override
        public void execute() throws IOException {
            if (args.contains("auto")) {
                session.setAutoRefresh(true);
                println("Enabled auto refresh", out);
            } else if (args.contains("manual")) {
                session.setAutoRefresh(false);
                println("Disabled auto refresh", out);
            } else if (args.trim().length() == 0) {
                session.refresh();
                println("Session refreshed", out);
            } else {
                println("Unrecognized arguments: " + args, out);
            }

        }
    }

    public static final class Checkpoint extends Command {

        public Checkpoint() {
            this.description = "Create a checkpoint.";
        }

        @Override
        public void execute() throws Exception {
            long time = TimeUnit.HOURS.toSeconds(1);
            if (args.trim().length() > 0) {
                try {
                    time = Long.parseLong(args.trim());
                } catch (NumberFormatException e) {
                    println("Not a number: " + args, out);
                    return;
                }
            }
            StringBuilder msg = new StringBuilder();
            msg.append("Checkpoint created: ");
            msg.append(session.checkpoint(time));
            msg.append(" (expires: ");
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.SECOND, (int) time);
            msg.append(cal.getTime().toString());
            msg.append(").");
            println(msg, out);
        }
    }

    public static final class Retrieve extends Command {

        public Retrieve() {
            this.description = "Retrieve a snapshot of the given checkpoint";
        }

        @Override
        public void execute() throws Exception {
            if (session.isAutoRefresh()) {
                session.setAutoRefresh(false);
                println("Auto refresh disabled.", out);
            }
            session.retrieve(args.trim());
            println("Root node is now at " + args.trim(), out);
        }
    }

    public static final class Eval extends Command {

        public Eval() {
            this.description = "Evaluate a script.";
        }

        @Override
        public void execute() throws Exception {
            String langExtn;
            Reader scriptReader;
            if (args.startsWith("-")) {
                int indexOfSpace = args.indexOf(' ');
                langExtn = args.substring(1, indexOfSpace);
                scriptReader = new StringReader(args.substring(indexOfSpace + 1));
            } else {
                File scriptFile = new File(args);
                if (!scriptFile.exists()) {
                    println("Script file not found: " + args, out);
                    return;
                }

                langExtn = FilenameUtils.getExtension(scriptFile.getName());
                scriptReader = new FileReader(scriptFile);
            }

            PrintWriter writer = new PrintWriter(out);

            ScriptEngineManager factory = new ScriptEngineManager();
            ScriptEngine engine = factory.getEngineByExtension(langExtn);

            if(engine == null){
                println("No script engine found for extension: " + langExtn, out);
                return;
            }

            engine.put("session", session);
            engine.put("out", writer);
            engine.eval(scriptReader);
            writer.flush();
        }
    }

    public static final class Exec extends Command {

        public Exec() {
            this.description = "Execute an operating system process.";
        }

        @Override
        public void execute() throws Exception {
            Process proc = new ProcessBuilder().command(args.split(" "))
                    .redirectErrorStream(true).start();
            IOPump inPump = IOPump.start(in, proc.getOutputStream());
            IOPump outPump = IOPump.start(proc.getInputStream(), out);
            proc.waitFor();
            inPump.join();
            outPump.join();
        }

    }

    //-----------------------< DocumentNodeStore specific >---------------------

    abstract static class DocumentNodeStoreCommand extends Command {
        @Override
        public void execute() throws IOException {
            if (session.getStore() instanceof DocumentNodeStore) {
                execute((DocumentNodeStore) session.getStore());
            } else {
                println("Can only execute command on a DocumentNodeStore", out);
            }
        }

        protected abstract void execute(@Nonnull DocumentNodeStore store)
                throws IOException;
    }

    public static final class Pd extends DocumentNodeStoreCommand {

        public Pd() {
            this.description = "Print the current document.";
        }

        @Override
        protected void execute(@Nonnull DocumentNodeStore store)
                throws IOException {
            String id = Utils.getIdFromPath(session.getWorkingPath());
            NodeDocument doc = store.getDocumentStore().find(NODES, id);
            if (doc == null) {
                println("[null]", out);
            } else {
                println(doc, out);
            }
        }

        private void println(NodeDocument doc, OutputStream out)
                throws IOException {
            int level = 1;
            Writer writer = new OutputStreamWriter(out);
            Set<String> mapKeys = Sets.newHashSet();
            writer.write('{');
            String comma = "";
            for (String key : doc.keySet()) {
                Object value = doc.get(key);
                if (value instanceof Map) {
                    mapKeys.add(key);
                    continue;
                }
                writer.write(comma);
                comma = ",";
                println(writer);
                printIndent(level, writer);
                printJson(key, value, writer);
            }
            for (String key : mapKeys) {
                writer.write(comma);
                comma = ",";
                println(writer);
                printIndent(level, writer);
                writer.write(JSONObject.escape(key));
                writer.write(": {");
                println((Map) doc.get(key), level + 1, writer);
                println(writer);
                printIndent(level, writer);
                writer.write("}");
            }
            println(writer);
            writer.write('}');
            println(writer);
            writer.flush();
        }

        private void println(Map map, int level, Writer writer)
                throws IOException {
            String comma = "";
            for (Object obj : map.keySet()) {
                String key = obj.toString();
                Object value = map.get(obj);
                writer.write(comma);
                comma = ",";
                println(writer);
                printIndent(level, writer);
                printJson(key, value, writer);
            }
        }

        private void printJson(String key, Object value, Writer writer)
                throws IOException {
            writer.write(JSONObject.escape(key));
            writer.write(": ");
            writer.write(jsonEscape(value));
        }

        private String jsonEscape(Object value) {
            String escaped = JSONValue.toJSONString(value);
            return escaped.replaceAll("\\\\/", "/");
        }

        private void printIndent(int level, Writer writer) throws IOException {
            for (int i = 0; i < level; i++) {
                writer.write(SPACES);
            }
        }
    }

    public static final class Lsd extends DocumentNodeStoreCommand {

        public Lsd() {
            this.description = "List the identifiers of the child documents at the current path.";
        }

        @Override
        protected void execute(@Nonnull DocumentNodeStore store)
                throws IOException {
            Writer writer = new OutputStreamWriter(out);
            String path = session.getWorkingPath();
            String fromKey = Utils.getKeyLowerLimit(path);
            String toKey = Utils.getKeyUpperLimit(path);
            int num = 0;
            for (NodeDocument doc : store.getDocumentStore().query(
                    NODES, fromKey, toKey, Integer.MAX_VALUE)) {
                writer.write(doc.getId());
                println(writer);
                num++;
            }
            println(writer);
            writer.write("Found " + num + " document");
            if (num != 1) {
                writer.write("s");
            }
            writer.write(".");
            println(writer);
            writer.flush();
        }
    }

}
