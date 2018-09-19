#!/bin/sh
#flintrock launch $1 &&
flintrock run-command $1 "wget central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar" &&
flintrock run-command $1 "wget central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.2/hadoop-aws-2.7.2.jar" &&
flintrock run-command $1 "mv /home/ec2-user/aws-java-sdk-1.7.4.jar /home/ec2-user/spark/jars/" &&
flintrock run-command $1 "mv /home/ec2-user/hadoop-aws-2.7.2.jar /home/ec2-user/spark/jars/" &&
flintrock copy-file $1 ./target/scala-2.11/movierank_2.11-0.1.jar /home/ec2-user/ &&
flintrock run-command $1 "AWS_ACCESS_KEY_ID=AKIAJNOSJVIZ75MZZKZQ AWS_SECRET_ACCESS_KEY=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 aws s3 cp s3://movierank-deploy-bucket/movies4g.txt movies4g.txt"
flintrock run-command $1 "cp movies4g.txt movies2g.txt && truncate -s 2GB movies2g.txt" &&
flintrock run-command $1 "cp movies2g.txt movies1g.txt && truncate -s 1GB movies1g.txt" &&
#flintrock run-command $1 "cp movies1g.txt movies500m.txt && truncate -s 500MB movies500m.txt" &&
#flintrock run-command $1 "cp movies500m.txt movies250m.txt && truncate -s 250MB movies250m.txt" &&
#flintrock run-command $1 "cp movies500m.txt movies100m.txt && truncate -s 100MB movies100m.txt" &&
#flintrock run-command $1 "cp movies500m.txt movies5m.txt && truncate -s 5MB movies5m.txt" &&
flintrock copy-file $1 ../../local_program/spark-2.3.1-bin-hadoop2.7/conf/log4j.properties /home/ec2-user/spark/conf/
#flintrock describe
