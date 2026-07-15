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
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

import java.util.concurrent.TimeUnit

@CompileStatic
class CheckpointCommand extends CommandSupport{
    public static final String COMMAND_NAME = 'checkpoint'

    public CheckpointCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'cp')
    }

    @Override
    Object execute(List<String> args) {
        long time = TimeUnit.HOURS.toSeconds(1);
        if (args) {
            time = args[0] as long
        }

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, (int) time);
        io.out.println "Checkpoint created: ${session.checkpoint(time)} (expires: ${cal.getTime().toString()})."
        return null
    }

    ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }
}
