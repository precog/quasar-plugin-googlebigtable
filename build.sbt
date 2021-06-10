ThisBuild / crossScalaVersions := Seq("2.12.10")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-plugin-googlebigtable"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-plugin-googlebigtable"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-plugin-googlebigtable"),
  "scm:git@github.com:precog/quasar-plugin-googlebigtable.git"))

ThisBuild / publishAsOSSProject := true

val precogGoogleAuthVersion = "0.0.1"
val bigTableVersion = "1.20.1"

val specs2Version = "4.10.5"
val catsEffectTestingSpecs2Version = "0.4.1"
val log4jVersion = "2.14.0"

lazy val quasarVersion =
  Def.setting[String](managedVersions.value("precog-quasar"))

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(datasource)

lazy val datasource = project
  .in(file("datasource"))
  .settings(
    name := "quasar-datasource-googlebigtable",

    quasarPluginName := "googlebigtable",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDatasourceFqcn := Some("quasar.plugin.googlebigtable.datasource.GoogleBigTableDatasourceModule$"),

    quasarPluginDependencies ++= Seq(
      "com.precog" %% "precog-google-auth" % precogGoogleAuthVersion,
      "com.google.cloud.bigtable" % "bigtable-hbase-1.x" % bigTableVersion
    ),

    libraryDependencies ++= Seq(
      "com.precog" %% "quasar-foundation"   % managedVersions.value("precog-quasar") % Test classifier "tests",
      "org.specs2" %% "specs2-core" % specs2Version % Test,
      "org.specs2"             %% "specs2-scalacheck"   % specs2Version % Test,
      "org.specs2"             %% "specs2-scalaz"       % specs2Version % Test,
      "com.codecommit" %% "cats-effect-testing-specs2" % catsEffectTestingSpecs2Version % Test,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion % Test,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % Test
    ))
  .enablePlugins(QuasarPlugin)
  .evictToLocal("QUASAR_PATH", "connector", true)
  .evictToLocal("QUASAR_PATH", "api", true)  
