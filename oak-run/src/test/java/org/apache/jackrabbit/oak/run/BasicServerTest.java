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
package org.apache.jackrabbit.oak.run;

import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.jackrabbit.util.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class BasicServerTest {

    private static final String SERVER_URL;

    static {
        String p = System.getProperty("jetty.http.port");
        if (p != null) {
            SERVER_URL = "http://localhost:" + p + "/";
        } else {
            SERVER_URL = Main.URI;
        }
    }

    private Main.HttpServer server;

    @Before
    public void startServer() throws Exception {
        server = new Main.HttpServer(SERVER_URL, new String[0]);
        server.start();
    }

    @After
    public void stopServer() throws Exception {
        server.stop();
    }

    @Test
    public void testServerOk() throws Exception {

        URL server = new URL(SERVER_URL);
        HttpURLConnection conn = (HttpURLConnection) server.openConnection();
        conn.setRequestProperty("Authorization",
                "Basic " + Base64.encode("a:a"));
        assertEquals(200, conn.getResponseCode());
    }
}
