package org.apache.jackrabbit.oak.explorer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import javax.swing.event.HyperlinkEvent;
import javax.swing.text.BadLocationException;
import javax.swing.text.DefaultHighlighter;
import javax.swing.text.Document;
import javax.swing.text.Highlighter;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class LogPanel {

    private static final Logger LOG = LoggerFactory.getLogger(LogPanel.class);

    private final JTextPane textPane = new JTextPane();
    private final List<LogPanelListener> listeners = new ArrayList<>();

    private JPanel panel;
    private String searchKeyword = "";

    public void addTarSelectedListener(LogPanelListener listener) {
        listeners.add(listener);
    }

    public void setHtmlContent(String text) {
        textPane.setContentType("text/html");
        textPane.setText(text);
        setHighlightOnTextPane();
        textPane.setCaretPosition(0);
    }

    public void setPlainContent(String text) {
        textPane.setContentType("text/plain");
        textPane.setText(text);
        setHighlightOnTextPane();
        textPane.setCaretPosition(0);
    }

    public JComponent getComponent() {
        if (panel == null) {
            createPanel();
        }
        return panel;
    }

    private void createPanel() {
        panel = new JPanel(new BorderLayout());
        panel.add(new JScrollPane(textPane), BorderLayout.CENTER);

        textPane.addHyperlinkListener(e -> {
            if (e.getEventType() == HyperlinkEvent.EventType.ACTIVATED) {
                onHyperLinkClickEvent(e);
            }
        });

        SearchPanel searchPanel = new SearchPanel();
        searchPanel.addSearchListener(this::onSearchKeywordChanged);

        panel.add(searchPanel.getComponent(), BorderLayout.SOUTH);
        textPane.setMargin(new Insets(5, 5, 5, 5));
        textPane.setEditable(false);
    }

    private void onHyperLinkClickEvent(HyperlinkEvent e) {
        try {
            if (e.getDescription().startsWith("tar://")) {
                String fileName = e.getDescription().replace("tar://", "");
                listeners.forEach(logPanelListener -> logPanelListener.onTarFileClicked(fileName));
            }
        } catch (Exception ex) {
            LOG.warn("Unable to open tar file", ex);
        }
    }

    private void onSearchKeywordChanged(String searchKeyword) {
        this.searchKeyword = searchKeyword;
        setHighlightOnTextPane();
    }

    private void setHighlightOnTextPane() {
        Highlighter highlighter = textPane.getHighlighter();
        highlighter.removeAllHighlights();
        Document document = textPane.getDocument();

        try {
            String renderedText = document.getText(0, document.getLength());
            if (searchKeyword != null && !searchKeyword.isEmpty()) {
                Pattern pattern = Pattern.compile(Pattern.quote(searchKeyword), Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(renderedText);
                while (matcher.find()) {
                    highlighter.addHighlight(matcher.start(), matcher.end(), DefaultHighlighter.DefaultPainter);
                }
            }
        } catch (BadLocationException e) {
            LOG.warn("Unable to highlight text", e);
        }
    }

    interface LogPanelListener {
        void onTarFileClicked(String tarFileName);
    }
}
