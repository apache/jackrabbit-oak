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

package org.apache.jackrabbit.oak.run.cli;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MutableClassToInstanceMap;
import com.google.common.collect.Sets;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public class Options {
    private final Set<OptionsBeanFactory> beanFactories = Sets.newHashSet();
    private final EnumSet<OptionBeans> oakRunOptions;
    private final ClassToInstanceMap<OptionsBean> optionBeans = MutableClassToInstanceMap.create();
    private OptionSet optionSet;
    private boolean disableSystemExit;
    private String commandName;
    private String summary;
    private String connectionString;
    private final Whiteboard whiteboard = new DefaultWhiteboard();

    public Options(){
        this.oakRunOptions = EnumSet.allOf(OptionBeans.class);
    }

    public Options(OptionBeans... options) {
        this.oakRunOptions = Sets.newEnumSet(asList(options), OptionBeans.class);
    }

    public OptionSet parseAndConfigure(OptionParser parser, String[] args) throws IOException {
        return parseAndConfigure(parser, args, true);
    }

    /**
     * Parses the arguments and configures the OptionBeans
     *
     * @param parser option parser instance
     * @param args command line arguments
     * @param checkNonOptions if true then it checks that non options are specified i.e. some NodeStore is
     *                        selected
     * @return optionSet returned from OptionParser
     */
    public OptionSet parseAndConfigure(OptionParser parser, String[] args, boolean checkNonOptions) throws IOException {
        for (OptionsBeanFactory o : Iterables.concat(oakRunOptions, beanFactories)){
            OptionsBean bean = o.newInstance(parser);
            optionBeans.put(bean.getClass(), bean);
        }
        parser.formatHelpWith(new OakHelpFormatter(optionBeans.values(), commandName, summary, connectionString));
        optionSet = parser.parse(args);
        configure(optionSet);
        checkForHelp(parser);
        if (checkNonOptions) {
            checkNonOptions();
        }
        return optionSet;
    }

    public OptionSet getOptionSet() {
        return optionSet;
    }

    @SuppressWarnings("unchecked")
    public <T extends OptionsBean> T getOptionBean(Class<T> clazz){
        Object o = optionBeans.get(clazz);
        checkNotNull(o, "No [%s] found in [%s]",
                clazz.getSimpleName(), optionBeans);
        return (T) o;
    }

    public void registerOptionsFactory(OptionsBeanFactory factory){
        beanFactories.add(factory);
    }

    public Options withDisableSystemExit() {
        this.disableSystemExit = true;
        return this;
    }

    public Options setCommandName(String commandName) {
        this.commandName = commandName;
        return this;
    }

    public Options setSummary(String summary) {
        this.summary = summary;
        return this;
    }

    public Options setConnectionString(String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    public CommonOptions getCommonOpts(){
        return getOptionBean(CommonOptions.class);
    }

    public Whiteboard getWhiteboard() {
        return whiteboard;
    }

    private void configure(OptionSet optionSet){
        for (OptionsBean bean : optionBeans.values()){
            bean.configure(optionSet);
        }
    }

    private void checkForHelp(OptionParser parser) throws IOException {
        if (optionBeans.containsKey(CommonOptions.class)
                && getCommonOpts().isHelpRequested()){
            parser.printHelpOn(System.out);
            systemExit(0);
        }
    }

    public void checkNonOptions() throws IOException {
        //Some non option should be provided to enable
        if (optionBeans.containsKey(CommonOptions.class)
                && getCommonOpts().getNonOptions().isEmpty()){
            System.out.println("NodeStore details not provided");
            systemExit(1);
        }
    }

    private void systemExit(int code) {
        if (!disableSystemExit) {
            System.exit(code);
        }
    }
}
