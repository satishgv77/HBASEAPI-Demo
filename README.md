# HBASEAPI-Demo-VIDSCALE

This is an extremely basic example of how to Insert rows in to a MapR DB table from Java.
Run this below command from any node on the cluster
java -cp java -cp .:$(hbase classpath):VIDScaleHBASEAPI-0.0.1-SNAPSHOT.jar com.mapr.ps.vidscale.TestApp

Run this below command from Drill after inserting data in to Mapr-DB table using the above
select convert_from(row_key,'UTF8') as row_key, CONVERT_FROM(property_global_hour.cf.bytes, 'DOUBLE_BE') as BYTES, 
CONVERT_FROM(property_global_hour.cf.uniq_vis, 'INT_BE') as uniq_vis from maprdb.property_global_hour

For trouble shooting the habse class path and for compiling the java files follow the below document 
https://www.mapr.com/developercentral/code/developing-m7-applications-maven#.V5uTEpOAOko
