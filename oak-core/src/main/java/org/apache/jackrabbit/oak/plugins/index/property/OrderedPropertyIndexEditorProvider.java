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

package org.apache.jackrabbit.oak.plugins.index.property;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@Service(IndexEditorProvider.class)
public class OrderedPropertyIndexEditorProvider implements IndexEditorProvider, OrderedIndex {
   private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndexEditorProvider.class);
   private int hits;
   private static int threshold = OrderedIndex.TRACK_DEPRECATION_EVERY;
   
   @Override
   @CheckForNull
   public Editor getIndexEditor(@Nonnull String type, 
                                @Nonnull NodeBuilder definition, 
                                @Nonnull NodeState root, 
                                @Nonnull IndexUpdateCallback callback) throws CommitFailedException {
        if (OrderedIndex.TYPE.equals(type)) {
            if (hit() % threshold == 0) {
                if (callback instanceof ContextAwareCallback) {
                    LOG.warn(DEPRECATION_MESSAGE, ((ContextAwareCallback)callback).getIndexingContext().getIndexPath());
                } else {
                    LOG.warn(OrderedIndex.DEPRECATION_MESSAGE, definition);
                }
            }
        }
        return null;
   }
   
   private synchronized int hit() {
       return hits++;
   }
   
   /**
    * used for testing purposes. Not thread safe.
    * @param t
    */
   static void setThreshold(int t) {
       threshold = t;
   }
}
