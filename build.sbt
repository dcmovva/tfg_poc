name := "TFGPoc"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.1.0" exclude("org.eclipse.jetty.orbit", "javax.servlet") exclude("org.eclipse.jetty.orbit", "javax.transaction") exclude("org.eclipse.jetty.orbit", "javax.activation") exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish") exclude("commons-beanutils", "commons-beanutils-core") exclude("commons-collections", "commons-collections") exclude("commons-logging", "commons-logging") exclude("com.esotericsoftware.minlog", "minlog")) 
