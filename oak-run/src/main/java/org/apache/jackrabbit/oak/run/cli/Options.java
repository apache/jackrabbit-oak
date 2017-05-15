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

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MutableClassToInstanceMap;
import com.google.common.collect.Sets;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public class Options {
    private final Set<OptionsBeanFactory> beanFactories = Sets.newHashSet();
    private final EnumSet<OptionBeans> oakRunOptions;
    private final ClassToInstanceMap<OptionsBean> optionBeans = MutableClassToInstanceMap.create();
    private OptionSet optionSet;

    public Options(){
        this.oakRunOptions = EnumSet.allOf(OptionBeans.class);
    }

    public Options(OptionBeans... options) {
        this.oakRunOptions = Sets.newEnumSet(asList(options), OptionBeans.class);
    }

    public OptionSet parseAndConfigure(OptionParser parser, String[] args){
        for (OptionsBeanFactory o : Iterables.concat(oakRunOptions, beanFactories)){
            OptionsBean bean = o.newInstance(parser);
            optionBeans.put(bean.getClass(), bean);
        }
        optionSet = parser.parse(args);
        configure(optionSet);
        return optionSet;
    }

    public OptionSet getOptionSet() {
        return optionSet;
    }

    public <T extends OptionsBean> T getOptionBean(Class<T> clazz){
        Object o = optionBeans.get(clazz);
        checkNotNull(o, "No [%s] found in [%s]",
                clazz.getSimpleName(), optionBeans);
        return (T) o;
    }

    public void registerOptionsFactory(OptionsBeanFactory factory){
        beanFactories.add(factory);
    }

    public CommonOptions getCommonOpts(){
        return getOptionBean(CommonOptions.class);
    }

    private void configure(OptionSet optionSet){
        for (OptionsBean bean : optionBeans.values()){
            bean.configure(optionSet);
        }
    }
}
