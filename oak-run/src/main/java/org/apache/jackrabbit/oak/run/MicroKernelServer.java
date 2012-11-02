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
package org.apache.jackrabbit.oak.run;

import java.net.InetAddress;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.server.Server;

public class MicroKernelServer {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.format("usage: %s /path/to/mk [port] [bindaddr]%n",
                    MicroKernelServer.class.getName());
            return;
        }

        final MicroKernelImpl mk = new MicroKernelImpl(args[0]);

        final Server server = new Server(mk);
        if (args.length >= 2) {
            server.setPort(Integer.parseInt(args[1]));
        } else {
            server.setPort(28080);
        }
        if (args.length >= 3) {
            server.setBindAddress(InetAddress.getByName(args[2]));
        }
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                server.stop();
                mk.dispose();
            }
        }, "ShutdownHook"));
    }

}
