@echo off
java -Dlog4j.configuration=file:target/classes/log4j.xml -Ds3s3mirror.version=1.2.8 -jar target/s3s3mirror-1.2.7-SNAPSHOT.jar %*
