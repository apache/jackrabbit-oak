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

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.*;
import javax.swing.UIManager.LookAndFeelInfo;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.IOUtils;

/**
 * NodeStore explorer
 * 
 * GUI based on Swing, for now it is tailored to the TarMK
 * 
 */
public class Explorer {

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

    public static void main(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        OptionSpec skipSizeCheck = parser.accepts("skip-size-check", "Don't compute the size of the records");
        OptionSpec<File> nonOptions = parser.nonOptions().ofType(File.class);
        OptionSet options = parser.parse(args);

        if (options.valuesOf(nonOptions).isEmpty()) {
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        File path = options.valuesOf(nonOptions).get(0);

        ExplorerBackend backend = new SegmentTarExplorerBackend(path);

        new Explorer(path, backend, options.has(skipSizeCheck));
    }

    private final ExplorerBackend backend;

    private Explorer(final File path, ExplorerBackend backend, final boolean skipSizeCheck) throws IOException {
        this.backend = backend;

        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                initLF();
                try {
                    createAndShowGUI(path, skipSizeCheck);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void createAndShowGUI(final File path, boolean skipSizeCheck) throws IOException {
        JTextArea log = new JTextArea(5, 20);
        log.setMargin(new Insets(5, 5, 5, 5));
        log.setLineWrap(true);
        log.setEditable(false);

        final NodeStoreTree treePanel = new NodeStoreTree(backend, log, skipSizeCheck);

        final JFrame frame = new JFrame("Explore " + path + " @head");
        frame.addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent windowEvent) {
                IOUtils.closeQuietly(treePanel);
                System.exit(0);
            }
        });

        JPanel content = new JPanel(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 1;
        c.weighty = 1;

        JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
                new JScrollPane(treePanel), new JScrollPane(log));
        splitPane.setDividerLocation(0.3);
        content.add(new JScrollPane(splitPane), c);
        frame.getContentPane().add(content);

        JMenuBar menuBar = new JMenuBar();
        menuBar.setMargin(new Insets(2, 2, 2, 2));

        JMenuItem menuReopen = new JMenuItem("Reopen");
        menuReopen.setMnemonic(KeyEvent.VK_R);
        menuReopen.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent ev) {
                try {
                    treePanel.reopen();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        JMenuItem menuCompaction = new JMenuItem("Time Machine");
        menuCompaction.setMnemonic(KeyEvent.VK_T);
        menuCompaction.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent ev) {
                List<String> revs = backend.readRevisions();
                String s = (String) JOptionPane.showInputDialog(frame,
                        "Revert to a specified revision", "Time Machine",
                        JOptionPane.PLAIN_MESSAGE, null, revs.toArray(),
                        revs.get(0));
                if (s != null && treePanel.revert(s)) {
                    frame.setTitle("Explore " + path + " @" + s);
                }
            }
        });

        JMenuItem menuRefs = new JMenuItem("Tar File Info");
        menuRefs.setMnemonic(KeyEvent.VK_I);
        menuRefs.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent ev) {
                List<String> tarFiles = new ArrayList<String>();
                for (File f : path.listFiles()) {
                    if (f.getName().endsWith(".tar")) {
                        tarFiles.add(f.getName());
                    }
                }

                String s = (String) JOptionPane.showInputDialog(frame,
                        "Choose a tar file", "Tar File Info",
                        JOptionPane.PLAIN_MESSAGE, null, tarFiles.toArray(),
                        tarFiles.get(0));
                if (s != null) {
                    treePanel.printTarInfo(s);
                }
            }
        });

        JMenuItem menuSCR = new JMenuItem("Segment Refs");
        menuSCR.setMnemonic(KeyEvent.VK_R);
        menuSCR.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent ev) {
                String s = JOptionPane.showInputDialog(frame,
                        "Segment References\nUsage: <segmentId>",
                        "Segment References", JOptionPane.PLAIN_MESSAGE);
                if (s != null) {
                    treePanel.printSegmentReferences(s);
                }
            }
        });

        JMenuItem menuDiff = new JMenuItem("SegmentNodeState diff");
        menuDiff.setMnemonic(KeyEvent.VK_D);
        menuDiff.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent ev) {
                String s = JOptionPane.showInputDialog(frame,
                        "SegmentNodeState diff\nUsage: <recordId> <recordId> [<path>]",
                        "SegmentNodeState diff", JOptionPane.PLAIN_MESSAGE);
                if (s != null) {
                    treePanel.printDiff(s);
                }
            }
        });

        JMenuItem menuPCM = new JMenuItem("Persisted Compaction Maps");
        menuPCM.setMnemonic(KeyEvent.VK_P);
        menuPCM.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent ev) {
                treePanel.printPCMInfo();
            }
        });

        menuBar.add(menuReopen);
        menuBar.add(new JSeparator(JSeparator.VERTICAL));
        menuBar.add(menuCompaction);
        menuBar.add(new JSeparator(JSeparator.VERTICAL));
        menuBar.add(menuRefs);
        menuBar.add(new JSeparator(JSeparator.VERTICAL));
        menuBar.add(menuSCR);
        menuBar.add(new JSeparator(JSeparator.VERTICAL));
        menuBar.add(menuDiff);
        menuBar.add(new JSeparator(JSeparator.VERTICAL));
        menuBar.add(menuPCM);
        menuBar.add(new JSeparator(JSeparator.VERTICAL));

        frame.setJMenuBar(menuBar);
        frame.pack();
        frame.setSize(960, 720);
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);

    }

}