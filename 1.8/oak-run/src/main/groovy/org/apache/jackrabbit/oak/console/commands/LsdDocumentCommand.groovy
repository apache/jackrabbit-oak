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

import groovy.transform.CompileStatic
import org.apache.jackrabbit.oak.console.ConsoleSession
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
import org.apache.jackrabbit.oak.plugins.document.NodeDocument
import org.apache.jackrabbit.oak.plugins.document.util.Utils
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES

@CompileStatic
class LsdDocumentCommand extends CommandSupport{
    public static final String COMMAND_NAME = 'list-identifiers'

    public LsdDocumentCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'lsd')
    }

    @Override
    Object execute(List<String> args) {
        assert session.store instanceof DocumentNodeStore
        PrintWriter writer = io.out
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

    ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }

    DocumentNodeStore getStore(){
        return (DocumentNodeStore)session.getStore();
    }
}
