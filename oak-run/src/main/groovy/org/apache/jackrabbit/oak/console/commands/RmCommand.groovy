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

import org.apache.jackrabbit.oak.spi.commit.CommitInfo
import org.apache.jackrabbit.oak.spi.commit.EmptyHook
import org.apache.jackrabbit.oak.spi.state.NodeStore


class RmCommand extends CommandSupport{
    public static final String COMMAND_NAME = 'remove'

    public RmCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'rm')
    }

    @Override
    protected List<Completer> createCompleters() {
        return [
                new SimpleCompletor(){
                    @Override
                    SortedSet getCandidates() {
                        SortedSet<String> names = new TreeSet<String>()
                        Iterables.limit(getSession().getWorkingNode().childNodeNames, 100).each {
                            names << it
                        }
                        return names
                    }
                },
                null
        ]
    }

    @Override
    Object execute(List<String> args) {
        if(args.isEmpty()){
            return;
        }

        String arg = args[0]?.trim()
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
        NodeStore ns = session.store
        def rnb = ns.root.builder()
        def nb = rnb;
        elements.each {
          if(it.size() > 0) {
            nb = nb.getChildNode(it)
          }
        }
        
        if (!nb.exists()) {
            io.out.println("No such node");
            return;
        }
        nb.remove();
        ns.merge(rnb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        io.out.println("Removed node ${path}");
        session.refresh();
    }

    ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }
}
