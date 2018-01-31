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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.HelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionSpec;
import joptsimple.internal.Strings;

import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;

public class OakHelpFormatter implements HelpFormatter {
    private static final int COL_WIDTH = 120;
    private final List<OptionsBean> optionBeans;
    private final String commandName;
    private final String connectionString;
    private final String summary;

    public OakHelpFormatter(Iterable<OptionsBean> optionBeans, @Nullable String commandName,
                            @Nullable String summary,@Nullable String connectionString) {
        this.optionBeans = Lists.newArrayList(optionBeans);
        this.commandName = commandName;
        this.summary = summary;
        this.connectionString = connectionString;
    }

    @Override
    public String format(Map<String, ? extends OptionDescriptor> options) {
        Map<String, ? extends OptionDescriptor> clonedOptions = Maps.newHashMap(options);
        List<OptionCategory> optionCategories = categorise(clonedOptions);
        //TODO Take care of left over options

        StringBuilder builder = new StringBuilder();
        builder.append(new MainSectionFormatter().format(options)).append(LINE_SEPARATOR.value());

        for (OptionCategory c : optionCategories){
            builder.append(c.format()).append(LINE_SEPARATOR.value());
        }

        return builder.toString();
    }

    private List<OptionCategory> categorise(Map<String, ? extends OptionDescriptor> options) {
        List<OptionCategory> result = new ArrayList<>();

        for (OptionsBean bean : optionBeans) {
            Map<String, OptionDescriptor> optsForThisBean = new HashMap<>();
            Map<String, OptionDescriptor> operationsForThisBean = new HashMap<>();
            for (String name : getOptionNames(bean)) {
                OptionDescriptor desc = options.remove(name);
                if (desc != null) {
                    if (bean.operationNames().contains(name)){
                        operationsForThisBean.put(name, desc);
                    } else {
                        optsForThisBean.put(name, desc);
                    }
                }
            }
            result.add(new OptionCategory(bean, optsForThisBean, operationsForThisBean));
        }
        Collections.sort(result, Collections.reverseOrder());
        return result;
    }

    private static int getColWidth() {
        return COL_WIDTH;
    }

    private static Set<String> getOptionNames(OptionsBean bean) {
        Set<String> names = new HashSet<>();
        try {
            for (Field field : bean.getClass().getDeclaredFields()) {
                if (OptionSpec.class.isAssignableFrom(field.getType())) {
                    field.setAccessible(true);
                    OptionSpec spec = (OptionSpec) field.get(bean);
                    names.addAll(spec.options());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return names;
    }


    private static class OptionCategory implements Comparable<OptionCategory> {
        final OptionsBean bean;
        final Map<String, ? extends OptionDescriptor> options;
        final Map<String, ? extends OptionDescriptor> operations;

        public OptionCategory(OptionsBean bean, Map<String, ? extends OptionDescriptor> options,
                              Map<String, OptionDescriptor> operations) {
            this.bean = bean;
            this.options = options;
            this.operations = operations;
        }

        public String format() {
            StringBuilder builder = new StringBuilder();
            builder.append(new CategoryFormatter(bean).format(options));
            if (!operations.isEmpty()) {
                builder.append(LINE_SEPARATOR.value());
                builder.append(new OperationsFormatter().format(operations));
            }
            return builder.toString();
        }

        @Override
        public int compareTo(OptionCategory that) {
            return Ints.compare(this.bean.order(), that.bean.order());
        }
    }

    private static class CategoryFormatter extends BuiltinHelpFormatter {
        final OptionsBean bean;

        public CategoryFormatter(OptionsBean bean) {
            super(getColWidth(), 2);
            this.bean = bean;
        }

        @Override
        protected void addRows(Collection<? extends OptionDescriptor> options) {
            addHeader();
            super.addRows(options);
        }

        @Override
        protected void addNonOptionsDescription(Collection<? extends OptionDescriptor> options) {
            //Noop call as category options do not specify non options
        }

        private void addHeader() {
            String title = bean.title();
            if (title != null) {
                addNonOptionRow(title);
                addNonOptionRow(Strings.repeat('=', title.length()));
            }

            if (bean.description() != null) {
                addNonOptionRow(bean.description());
            }
        }
    }

    /**
     * Used for rending options which are "operations"
     */
    private static class OperationsFormatter extends BuiltinHelpFormatter {

        public static final String OPERATIONS = "Operations";

        public OperationsFormatter() {
            super(getColWidth(), 2);
        }

        @Override
        protected void addHeaders(Collection<? extends OptionDescriptor> options) {
            addOptionRow(OPERATIONS, message( "description.header" ) );
            addOptionRow( Strings.repeat('-', OPERATIONS.length()), message( "description.divider" ) );
        }

        @Override
        protected void addNonOptionsDescription(Collection<? extends OptionDescriptor> options) {
            //Noop call as category options do not specify non options
        }
    }

    /**
     * Formatter for the first section of the help. It dumps the connection string, command
     * and summary only. No options are handled by this formatter
     */
    private class MainSectionFormatter extends BuiltinHelpFormatter {

        public MainSectionFormatter(){
            super(getColWidth(), 2);
        }

        @Override
        protected void addRows(Collection<? extends OptionDescriptor> options) {
            String firstLine = commandName != null ? commandName + " " : "";
            if (connectionString != null) {
                firstLine += connectionString;
            }

            addNonOptionRow(firstLine);

            if (summary != null) {
                addNonOptionRow(summary);
            }

            fitRowsToWidth();
        }
    }
}
