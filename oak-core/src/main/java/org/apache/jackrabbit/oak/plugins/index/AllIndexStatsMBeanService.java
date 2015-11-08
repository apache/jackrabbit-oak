package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.jmx.AllIndexStatsMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class AllIndexStatsMBeanService extends AnnotatedStandardMBean implements AllIndexStatsMBean {

    private static final Logger log = LoggerFactory.getLogger(AllIndexStatsMBeanService.class);

    public AllIndexStatsMBeanService() {
        super(AllIndexStatsMBean.class);
    }

    public String getName() {
        return "All Indexes Statistics";
    }

    @Override
    public String getOldestStartLastSuccessIndexedTime() {
        String currentLast = "";
        try {
            final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
            String queryName = "org.apache.jackrabbit.oak:*,type=\"IndexStats\"";
            Set<ObjectName> objectNames = platformMBeanServer.queryNames(new ObjectName(queryName), null);
            ArrayList<String> mbeanDates = new ArrayList<String>();
            for (ObjectName indexStatsMBeanName: objectNames) {
                mbeanDates.add((String) platformMBeanServer.getAttribute(indexStatsMBeanName, "StartLastSuccessIndexedTime"));
            }
            currentLast = giveOldestDate(mbeanDates);
        } catch (MalformedObjectNameException e) {
            log.error(e.getMessage(), e);
        } catch (AttributeNotFoundException e) {
            log.error(e.getMessage(), e);
        } catch (MBeanException e) {
            log.error(e.getMessage(), e);
        } catch (ReflectionException e) {
            log.error(e.getMessage(), e);
        } catch (InstanceNotFoundException e) {
            log.error(e.getMessage(), e);
        }
        return currentLast;
    }

    private String giveOldestDate(List<String> allDates) {
        Collections.sort(allDates);
        return allDates.iterator().next();
    }
}
