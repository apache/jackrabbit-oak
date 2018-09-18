/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.doc.doxia;

import org.apache.jackrabbit.oak.doc.doxia.jcr.Railroad;
import org.apache.maven.doxia.macro.AbstractMacro;
import org.apache.maven.doxia.macro.Macro;
import org.apache.maven.doxia.macro.MacroExecutionException;
import org.apache.maven.doxia.macro.MacroRequest;
import org.apache.maven.doxia.sink.Sink;
import org.codehaus.plexus.component.annotations.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * RailroadMacro macro that prints out the content of a file or a URL.
 */
@Component( role = Macro.class, hint = "railroad" )
public class RailroadMacro
        extends AbstractMacro
{
    private final Map<String, Railroad> railroadCache = new HashMap<String, Railroad>();


    /** {@inheritDoc} */
    public void execute( Sink sink, MacroRequest request ) throws MacroExecutionException {
        try {
            String fileName = (String) request.getParameter( "file" );
            required("file", fileName);
            getLog().debug("fileName: " + fileName);

            String topic = (String) request.getParameter("topic");
            required("topic", topic);
            getLog().debug("topic: " + topic);

            boolean setAnchor = true;
            String setAnchorParam = (String) request.getParameter( "setAnchor" );
            if ( setAnchorParam != null && !"".equals( setAnchorParam ) ) {
                setAnchor = Boolean.valueOf( setAnchorParam ).booleanValue();
            }
            getLog().debug("Set Anchor: " + setAnchor);

            boolean renderLink = false;
            String renderLinkParam = (String) request.getParameter( "renderLink" );
            if ( renderLinkParam != null && !"".equals( renderLinkParam ) ) {
                renderLink = Boolean.valueOf( renderLinkParam ).booleanValue();
            }
            getLog().debug("Render Link: " + renderLink);

            Railroad railroad = getRailroad(fileName);

            if (renderLink) {
                sink.link(railroad.getLink("#" + topic));
                sink.text(topic);
                sink.link_();
            } else {
                if (setAnchor) {
                    sink.rawText("<h2>");
                    sink.anchor(railroad.getLink(topic));
                    sink.anchor_();
                    sink.text(topic);
                    sink.rawText("</h2>");
                }
                String str = railroad.render(topic);
                if (str == null) {
                    throw new MacroExecutionException("NO RAILROAD FOR " + topic + " in " + fileName);
                } else {
                    sink.rawText(str);
                }
            }
        } catch (Exception e) {
            getLog().error("Error creating railroad: " + e.getMessage());
            throw new MacroExecutionException("Error creating railroad: " + e.getMessage(), e);
        }
    }

    private Railroad getRailroad(String fileName) throws Exception {
        Railroad railroad = railroadCache.get(fileName);

        if (railroad == null) {
            getLog().info("Creating railroad for " + fileName);
            railroad = new Railroad(fileName);
            railroadCache.put(fileName, railroad);
        }

        return railroad;
    }
}
