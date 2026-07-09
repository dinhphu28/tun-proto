ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "io.tunproto"
ThisBuild / version      := "1.0.0"

// yamux-core is published to Maven Local by the java build:
//   (cd ../java && mvn -q -o -pl yamux-core -am install)
ThisBuild / resolvers += Resolver.mavenLocal

// Apache-2.0 Akka pins (avoid the BSL). akka-http 10.2.10 (Apache 2.0),
// akka-stream/akka-actor 2.6.20 (Apache 2.0). Do NOT bump past these.
lazy val akkaHttpVersion = "10.2.10"
lazy val akkaVersion     = "2.6.20"

lazy val root = (project in file("."))
  .settings(
    name := "tun-proto-scala",

    // -release:21 makes the virtual-thread APIs (Thread.ofVirtual /
    // Executors.newVirtualThreadPerTaskExecutor) available and targets Java 21.
    scalacOptions ++= Seq(
      "-release", "21",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    ),

    // Fork so the forked JVM is a real Java 21 (virtual threads at runtime).
    Test / fork := true,
    run / fork  := true,

    libraryDependencies ++= Seq(
      // The reused JVM core (Maven Local). Pulls in Netty ByteBuf + Jackson transitively.
      "io.tunproto"       %  "yamux-core"       % "1.0.0",

      "com.typesafe.akka" %% "akka-http"        % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream"      % akkaVersion,
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor"       % akkaVersion,

      // Logging backend for Akka (avoids the SLF4J NOP warning).
      "ch.qos.logback"    %  "logback-classic"  % "1.2.13",

      // Tests
      "com.typesafe.akka" %% "akka-http-testkit"   % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion     % Test,
      "org.scalatest"     %% "scalatest"           % "3.2.18"        % Test
    )
  )
