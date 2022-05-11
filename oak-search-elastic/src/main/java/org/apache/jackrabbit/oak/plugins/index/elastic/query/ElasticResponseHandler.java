/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import co.elastic.clients.elasticsearch.core.search.Hit;

/**
 * Class to process Elastic response objects.
 */
public class ElasticResponseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticResponseHandler.class);

    private final PlanResult planResult;
    private final Filter filter;

    ElasticResponseHandler(@NotNull FulltextIndexPlanner.PlanResult planResult, @NotNull Filter filter) {
        this.planResult = planResult;
        this.filter = filter;
    }

    public String getPath(Hit<? extends JsonNode> hit) {
        return transformPath(hit.source().get(FieldNames.PATH).asText());
    }

    private String transformPath(String path) {
        String transformedPath = planResult.transformPath(("".equals(path)) ? "/" : path);

        if (transformedPath == null) {
            LOG.trace("Ignoring path {} : Transformation returned null", path);
            return null;
        }

        return transformedPath;
    }

    public boolean isAccessible(String path) {
        return filter.isAccessible(path);
    }
}
