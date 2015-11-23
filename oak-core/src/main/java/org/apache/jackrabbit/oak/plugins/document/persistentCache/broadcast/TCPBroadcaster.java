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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.broadcast;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A broadcast mechanism that uses TCP. It is mainly used for testing.
 */
public class TCPBroadcaster implements Broadcaster {

    static final Logger LOG = LoggerFactory.getLogger(TCPBroadcaster.class);
    private static final int TIMEOUT = 100;
    private static final int MAX_BUFFER_SIZE = 64;
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final byte[] key;
    private final int id = NEXT_ID.incrementAndGet();
    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<Listener>();
    private final ServerSocket serverSocket;
    private final ArrayList<Client> clients = new ArrayList<Client>();
    private final Thread discoverThread;
    private final Thread acceptThread;
    private final Thread sendThread;
    private final LinkedBlockingDeque<ByteBuffer> sendBuffer = new LinkedBlockingDeque<ByteBuffer>();
    private volatile boolean stop;
    
    public TCPBroadcaster(String config) {
        LOG.info("Init " + config);
        MessageDigest messageDigest;
        try {
            String[] parts = config.split(";");
            int startPort = 9800;
            int endPort = 9810;
            String key = "";
            String[] sendTo = {"sendTo", "localhost"};
            for (String p : parts) {
                if (p.startsWith("ports ")) {
                    String[] ports = p.split(" ");
                    startPort = Integer.parseInt(ports[1]);
                    endPort = Integer.parseInt(ports[2]);
                } else if (p.startsWith("key ")) {
                    key = p.split(" ")[1];
                } else if (p.startsWith("sendTo ")) {
                    sendTo = p.split(" ");
                }
            }                    
            sendTo[0] = null;
            messageDigest = MessageDigest.getInstance("SHA-256");
            this.key = messageDigest.digest(key.getBytes("UTF-8"));
            IOException lastException = null;
            ServerSocket server = null;
            for (int port = startPort; port <= endPort; port++) {
                if (server == null) {
                    try {
                        server = new ServerSocket(port);
                    } catch (IOException e) {
                        LOG.debug("Cannot open port " + port);
                        lastException = e;
                        // ignore
                    }
                }
                for (String send : sendTo) {
                    if (send != null && !send.isEmpty()) {
                        try {
                            Client c = new Client(send, port);
                            clients.add(c);
                        } catch (IOException e) {
                            LOG.debug("Cannot connect to " + send + " " + port);
                            // ignore
                        }                        
                    }
                }
            }
            if (server == null && lastException != null) {
                throw lastException;
            }
            server.setSoTimeout(TIMEOUT);
            serverSocket = server;
            LOG.info("Listening on port " + server.getLocalPort());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        acceptThread = new Thread(new Runnable() {
            @Override
            public void run() {
                accept();
            }
        }, "Oak TCPBroadcaster: accept #" + id);
        acceptThread.setDaemon(true);
        acceptThread.start();
        
        discoverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                discover();
            }
        }, "Oak TCPBroadcaster: discover #" + id);
        discoverThread.setDaemon(true);
        discoverThread.start();
        
        sendThread = new Thread(new Runnable() {
            @Override
            public void run() {
                send();
            }
        }, "Oak TCPBroadcaster: send #" + id);
        sendThread.setDaemon(true);
        sendThread.start();
        
    }
    
    void accept() {
        while (!stop) {
            try {
                final Socket socket = serverSocket.accept();
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final DataInputStream in = new DataInputStream(socket.getInputStream());
                            byte[] testKey = new byte[key.length];
                            in.readFully(testKey);
                            if (ByteBuffer.wrap(testKey).compareTo(ByteBuffer.wrap(key)) != 0) {
                                LOG.debug("Key mismatch");
                                socket.close();
                                return;
                            }
                            while (!socket.isClosed()) {
                                int len = in.readInt();
                                byte[] data = new byte[len];
                                in.readFully(data);
                                ByteBuffer buff = ByteBuffer.wrap(data);
                                int start = buff.position();
                                for (Listener l : listeners) {
                                    buff.position(start);
                                    l.receive(buff);
                                }
                            }
                        } catch (IOException e) {
e.printStackTrace();                            
                            // ignore
                        }
                    }
                };
                Thread t = new Thread(r, "Oak TCPBroadcaster: listener");
                t.setDaemon(true);
                t.start();
            } catch (SocketTimeoutException e) {
                // ignore
            } catch (IOException e) {
                if (!stop) {
                    LOG.warn("Receive failed", e);
                }
                // ignore
            }                   
        }
        try {        
            serverSocket.close();
        } catch (IOException e) {
            LOG.debug("Closed");
            // ignore
        }
    }
    
    void discover() {
        while (!stop) {
            for (Client c : clients) {
                c.tryConnect(key);
                if (stop) {
                    break;
                }
            }
        }
    }
    
    void send() {
        while (!stop) {
            try {
                ByteBuffer buff = sendBuffer.pollLast(10, TimeUnit.MILLISECONDS);
                if (buff != null && !stop) {
                    sendBuffer(buff);
                }
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
    
    @Override
    public void send(ByteBuffer buff) {
        ByteBuffer b = ByteBuffer.allocate(buff.remaining());
        b.put(buff);
        b.flip();
        while (sendBuffer.size() > MAX_BUFFER_SIZE) {
            sendBuffer.pollLast();
        }
        sendBuffer.push(b);
    }
    
    private void sendBuffer(ByteBuffer buff) {
        int len = buff.limit();
        byte[] data = new byte[len];
        buff.get(data);
        for (Client c : clients) {
            c.send(data);
            if (stop) {
                break;
            }
        }
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    @Override
    public void close() {
        if (!stop) {
            LOG.debug("Stopping");
            this.stop = true;
            try {
                serverSocket.close();
            } catch (IOException e) {
                // ignore
            }
            try {
                acceptThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            try {
                sendThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            try {
                discoverThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public boolean isRunning() {
        return !stop;
    }
    
    static class Client {
        final InetAddress address;
        final int port;
        DataOutputStream out;
        Client(String name, int port) throws UnknownHostException {
            this.address = InetAddress.getByName(name);
            this.port = port;
        }
        void send(byte[] data) {
            DataOutputStream o = out;
            if (o != null) {
                synchronized (o) {
                    try {
                        o.writeInt(data.length);
                        o.write(data);
                        o.flush();
                    } catch (IOException e) {
                        LOG.debug("Writing failed, port " + port, e);
                        try {
                            o.close();
                        } catch (IOException e1) {
                            // ignore
                        }
                        out = null;
                    }
                }
            }
        }
        void tryConnect(byte[] key) {
            DataOutputStream o = out;
            if (o != null || address == null) {
                return;
            }
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(address, port), TIMEOUT);
                o = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                o.write(key);
                o.flush();
                out = o;
                LOG.info("Connected to " + address + " port " + port + " k " + key[0]);
            } catch (IOException e) {
                // ok, done
            }
        }
    }

}
