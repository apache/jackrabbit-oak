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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A broadcast mechanism that uses UDP. It is mainly used for testing.
 */
public class UDPBroadcaster implements Broadcaster, Runnable {

    static final Logger LOG = LoggerFactory.getLogger(UDPBroadcaster.class);

    private final byte[] key;
    private final ArrayList<Listener> listeners = new ArrayList<Listener>();
    private final MulticastSocket socket;
    private final int port;
    private final InetAddress group;
    private final InetAddress sendTo;
    private final Thread thread;
    private final int messageLength = 32 * 1024;
    private final Cipher encryptCipher;
    private final Cipher decryptCipher;
    private volatile boolean stop;
    
    public UDPBroadcaster(String config) {
        LOG.info("init " + config);
        MessageDigest messageDigest;
        try {
            String[] parts = config.split(";");
            String group = "FF78:230::1234";
            int port = 9876;
            String key = "";
            boolean aes = false;
            String sendTo = null;
            for (String p : parts) {
                if (p.startsWith("port ")) {
                    port = Integer.parseInt(p.split(" ")[1]);
                } else if (p.startsWith("group ")) {
                    group = p.split(" ")[1];
                } else if (p.startsWith("key ")) {
                    key = p.split(" ")[1];
                } else if (p.equals("aes")) {
                    aes = true;
                } else if (p.startsWith("sendTo ")) {
                    sendTo = p.split(" ")[1];
                }
            }                    
            messageDigest = MessageDigest.getInstance("SHA-256");
            this.key = messageDigest.digest(key.getBytes());
            if (aes) {
                KeyGenerator kgen = KeyGenerator.getInstance("AES");
                kgen.init(128);
                SecretKey aesKey = kgen.generateKey();  
                encryptCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                encryptCipher.init(Cipher.ENCRYPT_MODE, aesKey);
                decryptCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                IvParameterSpec ivParameterSpec = new IvParameterSpec(aesKey.getEncoded());
                decryptCipher.init(Cipher.DECRYPT_MODE, aesKey, ivParameterSpec);
            } else {
                encryptCipher = null;
                decryptCipher = null;
            }
            socket = new MulticastSocket(port);
            this.group = InetAddress.getByName(group);
            this.sendTo = sendTo == null ? this.group : InetAddress.getByName(sendTo);
            this.port = port;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        thread = new Thread(this, "Oak UDPBroadcaster listener");
        thread.setDaemon(true);
        thread.start();
    }
    
    @Override
    public void run() {
        byte[] receiveData = new byte[messageLength];             
        try {
            socket.joinGroup(group);
        } catch (IOException e) {
            if (!stop) {
                LOG.warn("join group failed", e);
            }
            stop = true;
            return;
        }              
        while (!stop) {
            try {
                DatagramPacket receivePacket = new DatagramPacket(
                        receiveData, receiveData.length);                   
                socket.receive(receivePacket);
                int len = receivePacket.getLength();
                if (len < key.length) {
                    LOG.debug("too short");
                    continue;
                }
                if (!checkKey(receiveData)) {
                    LOG.debug("key mismatch");
                    continue;
                }
                ByteBuffer buff = ByteBuffer.wrap(receiveData);
                buff.limit(len);
                int start = key.length;
                for (Listener l : listeners) {
                    buff.position(start);
                    l.receive(buff);
                }
            } catch (IOException e) {
                if (!stop) {
                    LOG.warn("receive failed", e);
                }
                // ignore
            }                   
        }
        try {        
            socket.leaveGroup(group);
            socket.close();
        } catch (IOException e) {
            if (!stop) {
                LOG.warn("leave group failed", e);
            }
        }
    }
    
    private boolean checkKey(byte[] data) {
        for (int i = 0; i < key.length; i++) {
            if (key[i] != data[i]) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public void send(ByteBuffer buff) {
        int len = key.length + buff.limit();
        if (len >= messageLength) {
            // message too long: ignore
            return;
        }
        try {
            byte[] sendData = new byte[len];
            System.arraycopy(key, 0, sendData, 0, key.length);
            buff.get(sendData, key.length, buff.limit());
            DatagramPacket sendPacket = new DatagramPacket(sendData,
                sendData.length, sendTo, port);
            socket.send(sendPacket);
        } catch (IOException e) {
            if (!stop) {
                LOG.debug("send failed", e);
            }
            // ignore
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
    
    byte[] encrypt(byte[] data) {
        if (encryptCipher == null) {
            return data;
        }
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            CipherOutputStream cipherOutputStream = new CipherOutputStream(outputStream, encryptCipher);
            cipherOutputStream.write(data);
            cipherOutputStream.flush();
            cipherOutputStream.close();
            byte[] encryptedBytes = outputStream.toByteArray();
            return encryptedBytes;
        } catch (IOException e) {
            LOG.debug("encrypt failed", e);
            throw new RuntimeException(e);
        }
    }

    byte[] decrypt(byte[] data) {
        if (decryptCipher == null) {
            return data;
        }
        try {        
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ByteArrayInputStream inStream = new ByteArrayInputStream(data);
            CipherInputStream cipherInputStream = new CipherInputStream(inStream, decryptCipher);
            byte[] buf = new byte[1024];
            int bytesRead;
            while ((bytesRead = cipherInputStream.read(buf)) >= 0) {
                outputStream.write(buf, 0, bytesRead);
            }
            return outputStream.toByteArray();
        } catch (IOException e) {
            LOG.debug("decrypt failed", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (!stop) {
            this.stop = true;
            socket.close();
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public boolean isRunning() {
        return !stop;
    }

    @Override
    public void setBroadcastConfig(DynamicBroadcastConfig broadcastConfig) {
        // not yet implemented
    }

}
