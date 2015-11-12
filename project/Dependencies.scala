import sbt._

object Dependencies {
  val sparkVersion = "1.5.2"

  lazy val sparkDependencies = Seq(
    ("org.apache.spark" %% "spark-core" % sparkVersion)
      .exclude("org.mortbay.jetty", "servlet-api")
      .exclude("commons-beanutils", "commons-beanutils-core")
      .exclude("commons-collections", "commons-collections")
      .exclude("commons-logging", "commons-logging")
      .exclude("com.esotericsoftware.minlog", "minlog"),
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-catalyst" % sparkVersion).map(_ % "provided")

  lazy val mleapCoreDependencies = Seq("org.scalanlp" %% "breeze" % "0.11.2",
    "org.scalanlp" %% "breeze-natives" % "0.11.2",
    "io.spray" %% "spray-json" % "1.3.2")

  lazy val mleapRuntimeDependencies = mleapCoreDependencies

  lazy val mleapSparkDependencies = mleapCoreDependencies
    .union(sparkDependencies)
}