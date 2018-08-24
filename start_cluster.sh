#!/usr/bin/sh
flintrock launch $1 &&
flintrock run-command $1 "wget central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar" &&
flintrock run-command $1 "wget central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.2/hadoop-aws-2.7.2.jar" &&
flintrock run-command $1 "mv /home/ec2-user/aws-java-sdk-1.7.4.jar /home/ec2-user/spark/jars/" &&
flintrock run-command $1 "mv /home/ec2-user/hadoop-aws-2.7.2.jar /home/ec2-user/spark/jars/" &&
flintrock copy-file $1 target/scala-2.11/movierank_2.11-0.1.jar /home/ec2-user/ &&
flintrock describe