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
package org.apache.jackrabbit.oak.console

import groovy.transform.CompileStatic
import groovy.transform.TypeCheckingMode
import jline.Terminal
import jline.TerminalFactory
import jline.console.history.FileHistory
import org.apache.jackrabbit.oak.run.commons.Utils
import org.apache.jackrabbit.oak.console.commands.*
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
import org.codehaus.groovy.runtime.StackTraceUtils
import org.codehaus.groovy.tools.shell.*
import org.codehaus.groovy.tools.shell.Command as ShellCommand
import org.codehaus.groovy.tools.shell.commands.*
import org.codehaus.groovy.tools.shell.util.Logger
import org.codehaus.groovy.tools.shell.util.Preferences
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.AnsiConsole
import org.fusesource.jansi.AnsiRenderer

/**
 * Some part of logic is based on usage in org.codehaus.groovy.tools.shell.Main
 */
@CompileStatic
class GroovyConsole {
    static {
        try {
            // Install the system adapters
            AnsiConsole.systemInstall()

            // Register jline ansi detector
            Ansi.setDetector(new AnsiDetector())
        } catch (UnsatisfiedLinkError e){
            Logger.create(GroovyConsole.class).warn("Error loading console support. Some console features might not work properly. See " +
                    "https://issues.apache.org/jira/browse/OAK-5961 for details", e)
        }
    }

    private final ConsoleSession session
    private final Groovysh shell
    private final IO io;

    GroovyConsole(ConsoleSession session, IO io, Closeable closeable) {
        this.session = session
        this.io = io
        this.shell = prepareShell()
        addShutdownHook {
            if (shell.history) {
                shell.history.flush()
            }
            closeable.close()
        }
    }

    int run(){
        shell.run(null, null)
    }

    int execute(List<String> args){
        try {
            shell.execute(args.join(' '))
            return 0;
        }catch(Throwable t){
            t.printStackTrace();
            return 1;
        }
    }

    private Groovysh prepareShell() {
        Binding binding = new Binding()
        binding['session'] = session
        Groovysh sh = new OakSh(getClass().getClassLoader(),
                binding, io, this.&registerCommands)
        sh.imports << 'org.apache.jackrabbit.oak.plugins.document.*'
        sh.imports << 'org.apache.jackrabbit.oak.plugins.segment.*'
        return sh
    }

    private void registerCommands(Groovysh shell){
        List<? extends ShellCommand> commands = []
        commands.addAll([
                new ExitCommand(shell),
                new ImportCommand(shell),
                new DisplayCommand(shell),
                new ClearCommand(shell),
                new ShowCommand(shell),
                new InspectCommand(shell),
                new PurgeCommand(shell),
                new EditCommand(shell),
                new LoadCommand(shell),
                new SaveCommand(shell),
                new RecordCommand(shell),
                new HistoryCommand(shell),
                new AliasCommand(shell),
                new SetCommand(shell),
                // does not do anything
                //new ShadowCommand(shell),
                new RegisterCommand(shell),
                new DocCommand(shell),
        ]);

        commands.addAll([
                //Oak Commands
                new OakHelpCommand(shell),
                new CdCommand(shell),
                new CheckpointCommand(shell),
                new LsCommand(shell),
                new PnCommand(shell),
                new RefreshCommand(shell),
                new RetrieveCommand(shell),
                new LuceneCommand(shell),
                new ExportRelevantDocumentsCommand(shell),
                new ExportCommand(shell)
        ])

        if(session.store instanceof DocumentNodeStore){
            commands.addAll([
                    //Oak Commands
                    new PrintDocumentCommand(shell),
                    new LsdDocumentCommand(shell),
            ])
        }

        commands.each {ShellCommand command ->
            shell.register(command)
        }
    }

    private class OakSh extends Groovysh {
        private boolean colored = false

        OakSh(ClassLoader classLoader, Binding binding, IO io, Closure registrar) {
            super(classLoader, binding, io, registrar)
        }

        public String renderPrompt() {
            return AnsiRenderer.render( buildPrompt() )
        }

        //Following methods are copied because they are private in parent however
        //they are referred via method handle which somehow looks for method in
        //derived class. And we need to customize the welcome banner in run method
        private String buildPrompt() {
            def prefix = session.workingPath
            def groovyShellProperty = System.getProperty("groovysh.prompt")
            def groovyShellEnv = System.getenv("GROOVYSH_PROMPT")
            if (groovyShellProperty) {
                prefix = groovyShellProperty
            } else if (groovyShellEnv) {
                prefix = groovyShellEnv
            }
            return colored ? "@|bold,blue ${prefix}>|@ " : "${prefix}>"
        }

        private void displayError(final Throwable cause) {
            if (errorHook == null) {
                throw new IllegalStateException("Error hook is not set")
            }
            if (cause instanceof MissingPropertyException) {
                if (cause.type && cause.type.canonicalName == Interpreter.SCRIPT_FILENAME) {
                    io.err.println("@|bold,red Unknown property|@: " + cause.property)
                    return
                }
            }

            errorHook.call(cause)
        }

        @CompileStatic(TypeCheckingMode.SKIP)
        protected void maybeRecordError(Throwable cause) {
            RecordCommand record = registry[RecordCommand.COMMAND_NAME]

            if (record != null) {
                boolean sanitize = Preferences.sanitizeStackTrace

                if (sanitize) {
                    cause = StackTraceUtils.deepSanitize(cause);
                }

                record.recordError(cause)
            }
        }

        @CompileStatic(TypeCheckingMode.SKIP)
        int run(final String commandLine) {
            Terminal term = TerminalFactory.create()
            colored = term.ansiSupported
            if (log.debug) {
                log.debug("Terminal ($term)")
                log.debug("    Supported:  $term.supported")
                log.debug("    ECHO:       (enabled: $term.echoEnabled)")
                log.debug("    H x W:      ${term.getHeight()} x ${term.getWidth()}")
                log.debug("    ANSI:       ${term.isAnsiSupported()}")

                if (term instanceof jline.WindowsTerminal) {
                    jline.WindowsTerminal winterm = (jline.WindowsTerminal) term
                    log.debug("    Direct:     ${winterm.directConsole}")
                }
            }

            def code

            try {
                loadUserScript('groovysh.profile')

                // if args were passed in, just execute as a command
                // (but cygwin gives an empty string, so ignore that)
                if (commandLine != null && commandLine.trim().size() > 0) {
                    // Run the given commands
                    execute(commandLine)
                } else {
                    loadUserScript('groovysh.rc')

                    // Setup the interactive runner
                    runner = new InteractiveShellRunner(
                            this,
                            this.&renderPrompt as Closure)

                    // Setup the history
                    File histFile = new File(userStateDirectory, 'groovysh.history')
                    history = new FileHistory(histFile)
                    runner.setHistory(history)

                    // Setup the error handler
                    runner.errorHandler = this.&displayError

                    //
                    // TODO: See if we want to add any more language specific completions, like for println for example?
                    //

                    // Display the welcome banner
                    if (!io.quiet) {
                        int width = term.getWidth()

                        // If we can't tell, or have something bogus then use a reasonable default
                        if (width < 1) {
                            width = 80
                        }

                        io.out.println("@|green Jackrabbit Oak Shell|@ (${getProductInfo()}, " +
                                "JVM: ${System.properties['java.version']})")
                        io.out.println("Type '@|bold :help|@' or '@|bold :h|@' for help.")
                        io.out.println('-' * (width - 1))
                    }

                    // And let 'er rip... :-)
                    runner.run()
                }

                code = 0
            }
            catch (ExitNotification n) {
                log.debug("Exiting w/code: ${n.code}")

                code = n.code
            }
            catch (Throwable t) {
                io.err.println("FATAL : " + t)
                t.printStackTrace(io.err)

                code = 1
            }

            assert code != null // This should never happen

            return code
        }

    }

    private static String getProductInfo(){
        return Utils.getProductInfo(
                GroovyConsole.class.getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-run/pom.properties"));
    }
}
