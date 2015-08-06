=====================================
Welcome to Jackrabbit Web Application
=====================================

This is the Web Application component of the Apache Jackrabbit project.
This component provides servlets used to access a Jackrabbit repository:

    * RepositoryAccessServlet.java
    * LoggingServlet.java
    * RepositoryStartupServlet.java

In addition, the project contains 2 different WebDAV servlets:

    * SimpleWebdavServlet.java
      Adds WebDAV support (DAV 1,2) to your jackrabbit repository.
  
    * JCRWebdavServerServlet.java
      A servlet used to remote JSR170 calls via WebDAV. 
      IMPORTANT: Please note, that this servlet is not intended to provide 
      common WebDAV support to the repository. Instead the primary goal is to 
      remote JSR170 calls.
      For the corresponding client see -> jackrabbit-jcr2dav (work in progress).
