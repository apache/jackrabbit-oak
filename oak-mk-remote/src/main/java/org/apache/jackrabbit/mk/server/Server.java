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
package org.apache.jackrabbit.mk.server;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ServerSocketFactory;

import org.apache.jackrabbit.mk.api.MicroKernel;

/**
 * Server exposing a {@code MicroKernel}.
 */
public class Server {

    /** java.net.ServerSocket's default backlog size. */
    private static final int BACKLOG = 50;

    private final ServerSocketFactory ssFactory;

    private AtomicReference<MicroKernel> mkref;

    private AtomicBoolean started = new AtomicBoolean();

    private AtomicBoolean stopped = new AtomicBoolean();

    private ServerSocket ss;

    private ExecutorService es;

    private int port;

    private InetAddress addr;

    /**
     * Create a new instance of this class.
     *
     * @param mk micro kernel
     */
    public Server(MicroKernel mk) {
        this(mk, ServerSocketFactory.getDefault());
        this.mkref = new AtomicReference<MicroKernel>(mk);
    }

    /**
     * Create a new instance of this class.
     *
     * @param mk micro kernel
     */
    public Server(MicroKernel mk, ServerSocketFactory ssFactory) {
        this.mkref = new AtomicReference<MicroKernel>(mk);
        this.ssFactory = ssFactory;
    }

    /**
     * Set port number to listen to.
     *
     * @param port port numbern
     * @throws IllegalStateException if the server is already started
     */
    public void setPort(int port) throws IllegalStateException {
        if (started.get()) {
            throw new IllegalStateException("Server already started.");
        }
        this.port = port;
    }

    /**
     * Set bind address.
     */
    public void setBindAddress(InetAddress addr) throws IllegalStateException {
        if (started.get()) {
            throw new IllegalStateException("Server already started.");
        }
        this.addr = addr;
    }

    /**
     * Start this server.
     *
     * @throws IOException if an I/O error occurs
     */
    public void start() throws IOException {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        ss = createServerSocket();
        es = createExecutorService();

        new Thread(new Runnable() {
            @Override
            public void run() {
                accept();
            }
        }, "Acceptor").start();
    }

    void accept() {
        try {
            while (!stopped.get()) {
                final Socket socket = ss.accept();
                es.execute(new Runnable() {
                    @Override
                    public void run() {
                        process(socket);
                    }
                });
            }
        } catch (IOException e) {
            /* ignore */
        }
    }

    private ServerSocket createServerSocket() throws IOException {
        return ssFactory.createServerSocket(port, BACKLOG, addr);
    }

    private ExecutorService createExecutorService() {
        return Executors.newCachedThreadPool();
    }

    /**
     * Process a connection attempt by a client.
     *
     * @param socket client socket
     */
    void process(Socket socket) {
        try {
            socket.setTcpNoDelay(true);
        } catch (IOException e) {
            /* ignore */
        }

        HttpProcessor processor = new HttpProcessor(socket, new Servlet() {
            @Override
            public void service(Request request, Response response)
                    throws IOException {
                Server.this.service(request, response);
            }
        });

        try {
            processor.process();
        } catch (SocketTimeoutException e) {
            /* ignore */
        } catch (EOFException e) {
            /* ignore */
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Service a request.
     *
     * @param request request
     * @param response response
     * @throws IOException if an I/O error occurs
     */
    void service(Request request, Response response) throws IOException {
        if (request.getMethod().equals("POST")) {
            MicroKernelServlet.INSTANCE.service(mkref.get(), request, response);
        } else {
            FileServlet.INSTANCE.service(request, response);
        }
    }

    /**
     * Return the server's local socket address.
     *
     * @return socket address or {@code null} if the server is not started
     */
    public InetSocketAddress getAddress() {
        if (!started.get() || stopped.get()) {
            return null;
        }
        SocketAddress address = ss.getLocalSocketAddress();
        if (address instanceof InetSocketAddress) {
            InetSocketAddress isa = (InetSocketAddress) address;
            if (isa.getAddress().isAnyLocalAddress()) {
                try {
                    return new InetSocketAddress(
                            InetAddress.getByName("localhost"),
                            ss.getLocalPort());
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return isa;
            }
        }
        return null;
    }

    /**
     * Stop this server.
     */
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        if (es != null) {
            es.shutdown();
        }
        if (ss != null) {
            try {
                ss.close();
            } catch (IOException e) {
                /* ignore */
            }
        }
    }

}
