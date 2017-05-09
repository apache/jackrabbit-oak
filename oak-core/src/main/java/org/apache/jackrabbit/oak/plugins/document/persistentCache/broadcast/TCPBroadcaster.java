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
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final int id = NEXT_ID.incrementAndGet();

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<Listener>();
    private final ConcurrentHashMap<String, Client> clients = new ConcurrentHashMap<String, Client>();
    private final ArrayBlockingQueue<ByteBuffer> sendBuffer = new ArrayBlockingQueue<ByteBuffer>(MAX_BUFFER_SIZE * 2);

    private volatile DynamicBroadcastConfig broadcastConfig;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private Thread discoverThread;
    private Thread sendThread;
    private String ownListener;
    private String ownKeyUUID = UUID.randomUUID().toString();
    private byte[] ownKey = ownKeyUUID.getBytes(UTF8);
    
    private final AtomicBoolean stop = new AtomicBoolean(false);
    
    public TCPBroadcaster(String config) {
        LOG.info("Init " + config);
        init(config);
    }
    
    public void init(String config) {
        try {
            String[] parts = config.split(";");
            int startPort = 9800;
            int endPort = 9810;
            String key = "";
            
            // for debugging, this will send everything to localhost:
            // String[] sendTo = {"sendTo", "localhost"};
            
            // by default, only the entries in the clusterNodes 
            // collection are used:
            String[] sendTo = {"sendTo"};
            
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
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            if (key.length() > 0) {
                ownKey = messageDigest.digest(key.getBytes(UTF8));
            }
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
                            Client c = new Client(send, port, ownKey);
                            clients.put(send + ":" + port, c);
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
    
    @Override
    public void setBroadcastConfig(DynamicBroadcastConfig broadcastConfig) {
        this.broadcastConfig = broadcastConfig;
        HashMap<String, String> clientInfo = new HashMap<String, String>();
        clientInfo.put(DynamicBroadcastConfig.ID, ownKeyUUID);
        ServerSocket s = serverSocket;
        if (s != null) {
            String address = getLocalAddress();
            if (address != null) {
                ownListener = address + ":" + s.getLocalPort();
                clientInfo.put(DynamicBroadcastConfig.LISTENER, ownListener);
            }
        }
        broadcastConfig.connect(clientInfo);
    }
    
    static String getLocalAddress() {
        String bind = System.getProperty("oak.tcpBindAddress", null);
        try {
            InetAddress address;
            if (bind != null && !bind.isEmpty()) {
                address = InetAddress.getByName(bind);
            } else {
                address = InetAddress.getLocalHost();
            }
            return address.getHostAddress();
        } catch (UnknownHostException e) {
            return "";
        }
    }
    
    void accept() {
        while (isRunning()) {
            try {
                final Socket socket = serverSocket.accept();
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final DataInputStream in = new DataInputStream(socket.getInputStream());
                            byte[] testKey = new byte[ownKey.length];
                            in.readFully(testKey);
                            if (ByteBuffer.wrap(testKey).compareTo(ByteBuffer.wrap(ownKey)) != 0) {
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
                if (isRunning()) {
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
        while (isRunning()) {
            DynamicBroadcastConfig b = broadcastConfig;
            if (b != null) {
                readClients(b);
            }
            for (Client c : clients.values()) {
                c.tryConnect();
                if (!isRunning()) {
                    break;
                }
            }
            synchronized (stop) {
                if (isRunning()) {
                    try {
                        stop.wait(2000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }
    }
    
    void readClients(DynamicBroadcastConfig b) {
        List<Map<String, String>> list = b.getClientInfo();
        for(Map<String, String> m : list) {
            String listener = m.get(DynamicBroadcastConfig.LISTENER);
            String id = m.get(DynamicBroadcastConfig.ID);
            if (listener.equals(ownListener)) {
                continue;
            }
            // the key is the combination of listener and id,
            // because the same ip address / port combination
            // could be there multiple times for some time
            // (in case there is a old, orphan entry for the same machine)
            String clientKey = listener + " " + id;
            Client c = clients.get(clientKey);
            if (c == null) {
                int index = listener.lastIndexOf(':');
                if (index >= 0) {
                    String host = listener.substring(0, index);
                    int port = Integer.parseInt(listener.substring(index + 1));
                    try {
                        byte[] key = id.getBytes(UTF8);
                        c = new Client(host, port, key);
                        clients.put(clientKey, c);
                    } catch (UnknownHostException e) {
                        // ignore
                    }
                }
            }
        }
    }
    
    void send() {
        while (isRunning()) {
            try {
                ByteBuffer buff = sendBuffer.poll(10, TimeUnit.MILLISECONDS);
                if (buff != null && isRunning()) {
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
            sendBuffer.poll();
        }
        try {
            sendBuffer.add(b);
        } catch (IllegalStateException e) {
            // ignore - might happen once in a while,
            // if the buffer was not yet full just before, but now
            // many threads concurrently tried to add
        }
    }
    
    private void sendBuffer(ByteBuffer buff) {
        int len = buff.limit();
        byte[] data = new byte[len];
        buff.get(data);
        for (Client c : clients.values()) {
            c.send(data);
            if (!isRunning()) {
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
        if (isRunning()) {
            LOG.debug("Stopping");
            synchronized (stop) {
                stop.set(true);
                stop.notifyAll();
            }
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

    public final boolean isRunning() {
        return !stop.get();
    }
    
    static class Client {
        final String host;
        final int port;
        final byte[] key;
        DataOutputStream out;
        Client(String host, int port, byte[] key) throws UnknownHostException {
            this.host = host;
            this.port = port;
            this.key = key;
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
        void tryConnect() {
            DataOutputStream o = out;
            if (o != null || host == null) {
                return;
            }
            InetAddress address;
            try {
                address = InetAddress.getByName(host);
            } catch (UnknownHostException e1) {
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
