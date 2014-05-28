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
package org.apache.jackrabbit.oak.console.commands

import com.google.common.collect.Sets
import groovy.transform.CompileStatic
import org.apache.jackrabbit.oak.console.ConsoleSession
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
import org.apache.jackrabbit.oak.plugins.document.NodeDocument
import org.apache.jackrabbit.oak.plugins.document.util.Utils
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh
import org.json.simple.JSONObject
import org.json.simple.JSONValue

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES

@CompileStatic
class PrintDocumentCommand extends CommandSupport{
    public static final String COMMAND_NAME = 'print-document'

    public PrintDocumentCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'pd')
    }

    @Override
    Object execute(List<String> args) {
        assert session.store instanceof DocumentNodeStore
        String id = Utils.getIdFromPath(session.getWorkingPath());
        NodeDocument doc = store.getDocumentStore().find(NODES, id);
        if (doc == null) {
            io.out.println("[null]");
        } else {
            println(doc, io.out);
        }

        return null
    }

    ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }

    DocumentNodeStore getStore(){
        return (DocumentNodeStore)session.getStore();
    }

    private void println(NodeDocument doc, PrintWriter writer)
            throws IOException {
        int level = 1;
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
            writer.println();
            printIndent(level, writer);
            printJson(key, value, writer);
        }
        for (String key : mapKeys) {
            writer.write(comma);
            comma = ",";
            writer.println();
            printIndent(level, writer);
            writer.write(JSONObject.escape(key));
            writer.write(": {");
            println((Map) doc.get(key), level + 1, writer);
            writer.println();
            printIndent(level, writer);
            writer.write("}");
        }
        writer.println();
        writer.write('}');
        writer.println();
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
            writer.println();
            printIndent(level, writer);
            printJson(key, value, writer);
        }
    }

    private static void printJson(String key, Object value, Writer writer)
            throws IOException {
        writer.write(JSONObject.escape(key));
        writer.write(": ");
        writer.write(jsonEscape(value));
    }

    private static String jsonEscape(Object value) {
        String escaped = JSONValue.toJSONString(value);
        return escaped.replaceAll("\\\\/", "/");
    }

    private static void printIndent(int level, Writer writer) throws IOException {
        writer.write(' '* (level *4))
    }
}
