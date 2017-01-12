java -agentlib:jdwp=transport=dt_shmem,address=jdbconn,server=y,suspend=n -cp lib\antlr-2.7.5.jar;lib\log4j-1.2.13.jar;build\classes edu.caltech.nanodb.client.ExclusiveClient

