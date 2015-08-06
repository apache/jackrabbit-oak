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
package org.apache.jackrabbit.j2ee;

import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Servlet context listener that releases all remaining Derby resources
 * when the web application is undeployed. The resources are released only
 * if the Derby classes were loaded from within this webapp.
 *
 * @see <a href="https://issues.apache.org/jira/browse/JCR-1301">JCR-1301</a>
 */
public class DerbyShutdown implements ServletContextListener {

    public void contextInitialized(ServletContextEvent event) {
    }

    public void contextDestroyed(ServletContextEvent event) {
        ClassLoader loader = DerbyShutdown.class.getClassLoader();

        // Deregister all JDBC drivers loaded from this webapp
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            // Check if this driver comes from this webapp
            if (driver.getClass().getClassLoader() == loader) {
                try {
                    DriverManager.deregisterDriver(driver);
                } catch (SQLException ignore) {
                }
            }
        }

        // Explicitly tell Derby to release all remaining resources.
        // Use reflection to avoid problems when the Derby is not used.
        try {
            Class<?> monitorClass =
                loader.loadClass("org.apache.derby.iapi.services.monitor.Monitor");
            if (monitorClass.getClassLoader() == loader) {
                Method getMonitorMethod =
                    monitorClass.getMethod("getMonitor", new Class<?>[0]);
                Object monitor =
                    getMonitorMethod.invoke(null, new Object[0]);
                if (monitor != null) {
                    Method shutdownMethod =
                        monitor.getClass().getMethod("shutdown", new Class<?>[0]);
                    shutdownMethod.invoke(monitor, new Object[0]);
                }
            }
        } catch (Exception ignore) {
        }
    }

}