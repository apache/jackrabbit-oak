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

package org.apache.jackrabbit.oak.remote.osgi;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.remote.RemoteRepository;
import org.apache.jackrabbit.oak.remote.content.ContentRemoteRepository;
import org.apache.jackrabbit.oak.remote.http.RemoteServlet;
import org.osgi.service.http.HttpService;

import java.util.Map;

import static org.apache.felix.scr.annotations.ConfigurationPolicy.REQUIRE;

@Component(
        metatype = true,
        immediate = true,
        label = "Apache Jackrabbit Oak Remote HTTP API",
        description = "The HTTP binding of the Remote API for a Jackrabbit Oak repository"
)
@Properties({
        @Property(
                name = "url",
                value = {"/api"},
                label = "Mount URL",
                description = "Where the root application is exposed in the URL namespace"
        )
})
public class RemoteServletRegistration {

    @Reference
    private HttpService httpService;

    @Reference
    private ContentRepository contentRepository;

    @Activate
    public void activate(Map properties) {
        registerServlet(getUrl(properties));
    }

    @Deactivate
    public void deactivate(Map properties) {
        unregisterServlet(getUrl(properties));
    }

    private void registerServlet(String url) {
        try {
            httpService.registerServlet(url, getRemoteServlet(), null, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void unregisterServlet(String url) {
        httpService.unregister(url);
    }

    private String getUrl(Map properties) {
        return (String) properties.get("url");
    }

    private RemoteServlet getRemoteServlet() {
        return new RemoteServlet(getRemoteRepository());
    }

    private RemoteRepository getRemoteRepository() {
        return new ContentRemoteRepository(contentRepository);
    }

}