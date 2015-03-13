====================================================
Welcome to Jackrabbit Amazon WebServices Extension
====================================================

This is the Amazon Webservices Extension component of the Apache Jackrabbit project.
This component contains S3 Datastore which stores binaries on Amazon S3 (http://aws.amazon.com/s3).

====================================================
Build Instructions
====================================================
To build the latest SNAPSHOT versions of all the components
included here, run the following command with Maven 3:

    mvn clean install

To run testcases which stores in S3 bucket, please pass aws config file via system property. For e.g.

    mvn clean install  -DargLine="-Dconfig=/opt/cq/aws.properties"

Sample aws properties located at src/test/resources/aws.properties

====================================================
Configuration Instructions
====================================================
It require to configure aws.properties to configure S3 Datastore.
    <DataStore class="org.apache.jackrabbit.aws.ext.ds.S3DataStore">
        <param name="config" value="${rep.home}/aws.properties"/>
    </DataStore>
