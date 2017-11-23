/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

import com.google.common.collect.Lists;

public class IndexMBeanRegistration implements Registration {

    private final Whiteboard whiteboard;
    private final List<Registration> regs = Lists.newArrayList();

    public IndexMBeanRegistration(Whiteboard whiteboard) {
        this.whiteboard = whiteboard;
    }

    public void registerAsyncIndexer(AsyncIndexUpdate task, long delayInSeconds) {
        task.setIndexMBeanRegistration(this);
        Map<String, Object> config = ImmutableMap.<String, Object>of(
                AsyncIndexUpdate.PROP_ASYNC_NAME, task.getName(),
                "scheduler.name", AsyncIndexUpdate.class.getName() + "-" + task.getName()
        );
        regs.add(scheduleWithFixedDelay(whiteboard, task, config, delayInSeconds, true, true));
        regs.add(registerMBean(whiteboard, IndexStatsMBean.class,
                task.getIndexStats(), IndexStatsMBean.TYPE, task.getName()));
    }

    @Override
    public void unregister() {
        new CompositeRegistration(regs).unregister();
    }

}
