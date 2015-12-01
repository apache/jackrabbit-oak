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

import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.readRevisions;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;

import org.apache.commons.io.IOUtils;

/**
 * NodeStore explorer
 * 
 * GUI based on Swing, for now it is tailored to the TarMK
 * 
 */
public class Explorer {

    private static String skip = "skip-size-check";

    public static void main(String[] args) throws IOException {
        new Explorer(args);
    }

    public Explorer(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("usage: explore <path> [skip-size-check]");
            System.exit(1);
        }

        final File path = new File(args[0]);
        final boolean skipSizeCheck = args.length == 2
                && skip.equalsIgnoreCase(args[1]);

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

    private void createAndShowGUI(final File path, boolean skipSizeCheck)
            throws IOException {

        JTextArea log = new JTextArea(5, 20);
        log.setMargin(new Insets(5, 5, 5, 5));
        log.setLineWrap(true);
        log.setEditable(false);

        final NodeStoreTree treePanel = new NodeStoreTree(path, log, skipSizeCheck);

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
                List<String> revs = readRevisions(path);
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
                    return;
                }
            }
        });

        JMenuItem menuSCR = new JMenuItem("Segment Refs");
        menuSCR.setMnemonic(KeyEvent.VK_R);
        menuSCR.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent ev) {
                String s = (String) JOptionPane.showInputDialog(frame,
                        "Segment References\nUsage: <segmentId>",
                        "Segment References", JOptionPane.PLAIN_MESSAGE);
                if (s != null) {
                    treePanel.printSegmentReferences(s);
                    return;
                }
            }
        });

        JMenuItem menuDiff = new JMenuItem("SegmentNodeState diff");
        menuDiff.setMnemonic(KeyEvent.VK_D);
        menuDiff.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent ev) {
                String s = (String) JOptionPane.showInputDialog(frame,
                        "SegmentNodeState diff\nUsage: <recordId> <recordId> [<path>]",
                        "SegmentNodeState diff", JOptionPane.PLAIN_MESSAGE);
                if (s != null) {
                    treePanel.printDiff(s);
                    return;
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
