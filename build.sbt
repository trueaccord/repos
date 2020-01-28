import ReleaseTransformations._

val SlickVersion = "3.2.3"

val AkkaVersion = "2.5.22"

val AkkaHttpVersion = "10.1.8"

lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

scalaVersion := scala211

organization := "com.trueaccord.repos"

name := "repos"

releasePublishArtifactsAction := PgpKeys.publishSigned.value

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _)),
  setNextVersion,
  commitNextVersion,
  pushChanges,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _))
)

libraryDependencies ++= Seq(
    "commons-codec" % "commons-codec" % "1.8",
    "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.slick" %% "slick" % SlickVersion,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-lang.modules" %% "scala-async" % "0.9.7",
    "org.xerial.snappy" % "snappy-java" % "1.1.1.6",
    "com.github.shyiko" % "mysql-binlog-connector-java" % "0.4.1",
    "org.slf4j" % "slf4j-api" % "1.7.5",

    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % "test",
    "com.h2database" % "h2" % "1.4.189" % "test",
    "org.scalactic" %% "scalactic" % "3.1.0" % "test",
    "org.scalatest" %% "scalatest" % "3.1.0" % "test",
    "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",
    "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2" % "test"
)

lazy val root = (project in file("."))
  .enablePlugins(SbtTwirl)
  .settings(crossScalaVersions := supportedScalaVersions)


