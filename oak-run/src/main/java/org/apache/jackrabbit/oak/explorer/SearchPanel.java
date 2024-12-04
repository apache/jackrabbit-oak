package org.apache.jackrabbit.oak.explorer;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.util.ArrayList;
import java.util.List;

class SearchPanel {

    private final List<SearchListener> listeners = new ArrayList<>();
    private JComponent component;

    public JComponent getComponent() {
        if (component == null) {
            component = createComponent();
        }
        return component;
    }

    public void addSearchListener(SearchListener searchListener) {
        listeners.add(searchListener);
    }

    private JComponent createComponent() {
        JPanel panel = new JPanel();
        JTextField textField = new JTextField(20);
        textField.putClientProperty("JTextField.placeholderText", "Search");

        textField.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                listeners.forEach(searchListener -> searchListener.search(textField.getText()));
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                listeners.forEach(searchListener -> searchListener.search(textField.getText()));
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                listeners.forEach(searchListener -> searchListener.search(textField.getText()));
            }
        });
        panel.add(textField);

        return panel;
    }

    public interface SearchListener {
        void search(String text);
    }

}
