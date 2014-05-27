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
package org.apache.jackrabbit.oak.explorer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.File;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;

import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;

/**
 * NodeStore explorer
 * 
 * GUI based on Swing, for now it is tailored to the TarMK
 * 
 */
public class Explorer {

    public static void main(String[] args) throws IOException {
        new Explorer(args);
    }

    public Explorer(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("usage: explore <path>");
            System.exit(1);
        }

        final String path = args[0];
        final FileStore store = new FileStore(new File(path), 256);

        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                initLF();
                createAndShowGUI(path, store);
            }
        });
    }

    private static void initLF() {
        try {
            for (LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (Exception e) {
            // If Nimbus is not available, you can set the GUI to another look
            // and feel.
        }
    }

    private void createAndShowGUI(String path, FileStore store) {
        // Create and set up the window.
        JFrame frame = new JFrame("Explore " + path);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JPanel content = new JPanel(new GridBagLayout());

        JTextArea log = new JTextArea(5, 20);
        log.setMargin(new Insets(5, 5, 5, 5));
        log.setLineWrap(true);
        log.setEditable(false);

        NodeStoreTree treePanel = new NodeStoreTree(store, log);

        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 1;
        c.weighty = 1;
        content.add(new JScrollPane(treePanel), c);

        c.weightx = 3;
        content.add(new JScrollPane(log), c);

        frame.getContentPane().add(content);

        frame.pack();
        frame.setSize(800, 600);
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);

    }

}
