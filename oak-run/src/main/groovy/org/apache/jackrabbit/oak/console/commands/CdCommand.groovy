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

import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import jline.console.completer.Completer
import org.apache.jackrabbit.oak.commons.PathUtils
import org.apache.jackrabbit.oak.console.ConsoleSession
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.util.SimpleCompletor

class CdCommand extends CommandSupport{
    public static final String COMMAND_NAME = 'change-dir'

    public CdCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'cd')
    }

    @Override
    protected List<Completer> createCompleters() {
        SimpleCompletor completor = new SimpleCompletor(){
            @Override
            SortedSet getCandidates() {
                SortedSet<String> names = new TreeSet<String>()
                Iterables.limit(getSession().getWorkingNode().childNodeNames, 100).each {
                    names << it.replace(" ", "\\ ")
                }
                return names
            }
        }
        completor.delimiter = '/'
        return [completor, null]
    }

    @Override
    Object execute(List<String> args) {
        if(args.isEmpty()){
            return;
        }

        String arg = args.join(' ').replace("\\ ", " ")
        if (!PathUtils.isValid(arg)) {
            io.out.println("Not a valid path: " + args);
            return;
        }
        String path;
        if (PathUtils.isAbsolute(arg)) {
            path = arg;
        } else {
            path = PathUtils.concat(session.getWorkingPath(), arg);
        }
        List<String> elements = Lists.newArrayList();
        PathUtils.elements(path).each{String element ->
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
            io.out.println("No such node ["+path+"]");
        }
    }

    ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }
}
