@echo off

set CPATH=.
set CPATH=%CPATH%;lib\log4j-1.2.13.jar
set CPATH=%CPATH%;lib\antlr-3.2.jar
set CPATH=%CPATH%;lib\commons-io-2.1.jar
set CPATH=%CPATH%;build\classes

rem To set the page-size to use, add "-Dnanodb.pagesize=2048" to JAVA_OPTS.
rem To enable transaction processing, add "-Dnanodb.txns=on" to JAVA_OPTS.
set JAVA_OPTS=-Dlog4j.configuration=logging.conf

java %JAVA_OPTS% -cp %CPATH% edu.caltech.nanodb.client.ExclusiveClient
