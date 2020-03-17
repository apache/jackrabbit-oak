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

import org.apache.jackrabbit.oak.commons.PathUtils
import org.apache.jackrabbit.oak.console.ConsoleSession
import org.apache.jackrabbit.oak.plugins.document.Collection
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
import org.apache.jackrabbit.oak.plugins.document.DocumentStore
import org.apache.jackrabbit.oak.plugins.document.NodeDocument
import org.apache.jackrabbit.oak.plugins.document.util.Utils
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

class ExportRelevantDocumentsCommand extends CommandSupport {
    public static final String COMMAND_NAME = 'export-docs'

    private DocumentNodeStore store;
    private DocumentStore docStore;

    private String path;
    private String out = 'all-required-nodes.json'

    public ExportRelevantDocumentsCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'exportd')
    }

    @Override
    Object execute(List<String> args) {
        if (parseOpts(args)) {
            initStores()
            exportDocs()
        }
    }

    private boolean parseOpts(List<String> args) {
        def cli = new CliBuilder(usage: getName() + ' ' + getUsage(), header: getHelp())

        cli.h(longOpt: 'help', 'Print usage')
        cli.o(longOpt: 'out', args: 1, argName: 'out',
                'File name to export documents in (default: ' + out + ')')
        cli.p(longOpt: 'path', args: 1, argName: 'path', 'Repository path to export (default: current node)')

        def options = cli.parse(args)

        if (!options) {
            return false
        }

        if (options.h) {
            cli.usage()
            return false
        }

        path = options.path ?: session.getWorkingPath()
        if (!PathUtils.isAbsolute(path)) {
            path = PathUtils.concat(session.getWorkingPath(), path);
        }

        out = options.out ?: out

        return true
    }

    private void initStores() {
        store = (DocumentNodeStore)getSession().getStore()
        docStore = store.getDocumentStore()
    }

    private void exportDocs() {
        File file = new File(out)
        file.delete()

        file = new File(out)

        io.println("Exporting " + path + " to " + file.absolutePath)

        writeOutDocAndAncestors(file)
    }

    private void writeOutDocAndAncestors(File file) {
        String currPath = path

        writeOutDocAndSplits(currPath, file)
        while (!PathUtils.denotesRoot(currPath)) {
            currPath = PathUtils.getParentPath(currPath)
            writeOutDocAndSplits(currPath, file)
        }
    }

    private void writeOutDocAndSplits(String currPath, File file) {
        NodeDocument doc = docStore.find(Collection.NODES, Utils.getIdFromPath(currPath))

        if (doc != null) {
            writeOutDoc(doc, file)

            for (NodeDocument splitDoc : doc.getAllPreviousDocs()) {
                writeOutDoc(splitDoc, file)
            }
        }
    }

    private void writeOutDoc(NodeDocument doc, File file) {
        file << '{'
        file << doc.asString()
        file << '}'
        file << System.getProperty("line.separator")
    }

    ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }
}
