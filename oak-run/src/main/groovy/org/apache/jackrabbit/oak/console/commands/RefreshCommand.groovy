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
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

@CompileStatic
class RefreshCommand extends ComplexCommandSupport{
    public static final String COMMAND_NAME = 'refresh'

    public RefreshCommand(Groovysh shell) {
        super(shell, COMMAND_NAME, 'rf', ['auto', 'manual', 'now'], 'now')
    }

    def do_auto = {
        getSession().autoRefresh = true
        io.out.println("Enabled auto refresh")
    }

    def do_manual = {
        getSession().autoRefresh = false
        io.out.println("Disabled auto refresh")
    }

    def do_now = {
        getSession().refresh()
        io.out.println("Session refreshed")
    }

    ConsoleSession getSession(){
        return (ConsoleSession)variables.session
    }
}
