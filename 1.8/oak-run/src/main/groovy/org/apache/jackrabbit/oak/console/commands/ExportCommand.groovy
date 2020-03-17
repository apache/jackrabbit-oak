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
import org.apache.jackrabbit.oak.exporter.NodeStateSerializer
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

class ExportCommand extends CommandSupport{
    public static final String COMMAND_NAME = 'export-nodes'

    private NodeStateSerializer serializer
    private String filter = '''{"properties":["*", "-:childOrder"]}'''
    private String out = '.'
    private boolean writeToConsole
    private String path

    ExportCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, "export")
    }

    @Override
    Object execute(List<String> args) {
        if (parseOpts(args)) {
            exportNodes()
        }
    }

    @Override
    String getHelp() {
        CliBuilder b = createCli(true)
        StringWriter sw = new StringWriter()
        b.writer = new PrintWriter(sw)
        b.usage()
        return sw.toString()
    }

    Object exportNodes() {
        if (writeToConsole) {
            io.println(serializer.serialize())
        } else {
            File file = new File(out)
            serializer.serialize(file)
            File json = new File(file, serializer.fileName)
            io.println("Exporting $path to ${json.absolutePath}")
        }
    }

    private boolean parseOpts(List<String> args) {
        def cli = createCli(false)

        def options = cli.parse(args)

        if (!options) {
            return false
        }

        if (options.h) {
            cli.usage()
            return false
        }

        writeToConsole = options.console
        out = options.out ?: out

        serializer = new NodeStateSerializer(session.root)

        path = options.path ?: session.getWorkingPath()
        if (!PathUtils.isAbsolute(path)) {
            path = PathUtils.concat(session.getWorkingPath(), path)
        }
        serializer.path = path
        serializer.depth = options.d ? options.d as int : Integer.MAX_VALUE
        serializer.maxChildNodes = options.n ? options.n as int : Integer.MAX_VALUE
        serializer.setSerializeBlobContent(options.b as boolean)
        serializer.filter = options.f ?: filter

        return true
    }

    private def createCli(boolean forHelp){
        def cli
        if (forHelp) {
            cli = new CliBuilder(header: getDescription())
        } else {
            cli = new CliBuilder(usage: getName() + ' ' + getUsage(), header: getDescription())
        }

        cli.h(longOpt: 'help', 'Print usage')
        cli.c(longOpt: 'console', 'Output to console')
        cli.b(longOpt: 'blobs', 'Serialize blob contents also')
        cli.n(longOpt: 'max-child-nodes', args:1, 'maximum number of child nodes to include')
        cli.f(longOpt: 'filter', args:1, "Filter for nodes and properties to include in json format. Default $filter")
        cli.d(longOpt: 'depth', args: 1, 'Maximum tree depth to write out. Default to all')
        cli.o(longOpt: 'out', args: 1, argName: 'out',
                'Directory name to store json and blobs (default: ' + out + ')')
        cli.p(longOpt: 'path', args: 1, argName: 'path', 'Repository path to export (default: current node)')
        return cli
    }

    private ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }
}