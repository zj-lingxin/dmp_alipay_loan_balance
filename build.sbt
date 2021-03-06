name := "dmp_alipay_loan_balance"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.4.0"

libraryDependencies += "com.rabbitmq" % "amqp-client" % "3.5.4"