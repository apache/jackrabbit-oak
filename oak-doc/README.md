Oak Documentation
=================

The Oak documentation lives as Markdown files in `src/site/markdown` such that it easy to view
e.g. from GitHub. Alternatively the Maven site plugin can be used to build and deploy a web site
as follows:

From the reactor use

    mvn site

to build the site without Javadoc or

    mvn site -Psite-with-javadoc

to build the site with Javadoc. Review the site at `oak-doc/target/site`.

Then deploy the site to `http://jackrabbit.apache.org/oak/docs/` using

    mvn site-deploy

Finally review the site at `http://jackrabbit.apache.org/oak/docs/index.html`.

Every committer should be able to deploy the site. No fiddling with credentials needed since
deployment is done via svn commit to `https://svn.apache.org/repos/asf/jackrabbit/site/live/oak/docs`.
