---------------------------------
Oak Performance Test Suite
---------------------------------

This directory contains a simple performance test suite that can be
extended for ongoing Oak versions and micro kernels. Use the following
command to run this test suite:

    mvn clean install

Note that the test suite will take more than an hour to complete, and to
avoid distorting the results you should avoid putting any extra load on
the computer while the test suite is running.

The results are stored as oak*/target/*.txt report files and can
be combined into an HTML report by running the following command on a
(Unix) system where gnuplot is installed.

    sh plot.sh

Mac OS X note : if you want to execute the above script, you will need
to install gnuplot and imagemagick2-svg from the Fink project. For
more information : http://finkproject.org 

Selecting which tests to run
----------------------------

The -Donly command line parameter allows you to specify a regexp for
selecting which performance test cases to run. To run a single test
case, use a command like this:

    mvn clean install -Donly=ConcurrentReadTest

To run all concurrency tests, use:

    mvn clean install -Donly=Concurrent.*Test

Selecting which micro kernel to test
----------------------------------------------------------

The -Dmk command line parameter allows you to specify a regexp for
selecting the micro kernel and configurations against which the
performance tests are run. The default setting selects only the default
micro kernel:

    mvn clean install -Dmk=\d\.\d

To run the tests against all included configurations, use:

    mvn clean install -Dmk=.*

Using a profiler
----------------

To enable a profiler, use the -Dagentlib= command line pameter:

    mvn clean install -Dagentlib=hprof=cpu=samples,depth=10

Adding a new performance test
-----------------------------

The tests run by this performance test suite are listed in the
testPerformance() method of the AbstractPerformanceTest class in
the org.apache.jackrabbit.oak.performance package of the oak-perf-base
component that you can find in the ./base directory.

Each test is a subclass of the AbstractTest class in that same package,
and you need to implement at least the abstract runTest() method when
creating a new test. The runTest() method should contain the code whose
performance you want to measure. For best measurement results the method
should normally take something between 0.1 to 10 seconds to execute, so
you may need to add a constant-size loop around your code like is done
for example in the LoginTest class. The test suite compares relative
performance between different Oak versions, so the absolute time
taken by the test method is irrelevant.

Many performance tests need some setup and teardown code for things like
building the content tree against which the test is being run. Such work
should not be included in the runTest() method to prevent affecting the
performance measurements. Instead you can override the before/afterTest()
and before/afterSuite() methods that get called respectively before and
after each individual test iteration and the entire test suite. See for
example the SetPropertyTest class for an example of how these methods
are best used.

