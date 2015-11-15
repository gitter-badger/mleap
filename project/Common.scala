import sbt._
import Keys._

object Common {
  val appVersion = "0.1-SNAPSHOT"

  val settings = Seq(
    version := appVersion,
    scalaVersion := "2.10.6",
    organization := "org.apache.mleap",
    scalacOptions ++= Seq("-unchecked", "-deprecation"))
}