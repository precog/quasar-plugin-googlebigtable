ThisBuild / crossScalaVersions := Seq("2.12.10")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-plugin-googlebigtable"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-plugin-googlebigtable"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-plugin-googlebigtable"),
  "scm:git@github.com:precog/quasar-plugin-googlebigtable.git"))

ThisBuild / publishAsOSSProject := true

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
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-plugin-googlebigtable",
    libraryDependencies ++= Seq(
      "com.codecommit" %% "cats-effect-testing-specs2" % catsEffectTestingSpecs2Version % Test,
      "org.specs2" %% "specs2-core" % specs2Version % Test
    ))

lazy val datasource = project
  .in(file("datasource"))
  .dependsOn(core % BothScopes)
  .settings(
    name := "quasar-datasource-googlebigtable",

    quasarPluginName := "googlebigtable",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDatasourceFqcn := Some("quasar.plugin.googlebigtable.datasource.GoogleBigTableDatasourceModule$"),

    quasarPluginDependencies ++= Seq(),

    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % specs2Version % Test,
      "com.codecommit" %% "cats-effect-testing-specs2" % catsEffectTestingSpecs2Version % Test,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion % Test,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % Test
    ))
  .enablePlugins(QuasarPlugin)
  .evictToLocal("QUASAR_PATH", "connector", true)
  .evictToLocal("QUASAR_PATH", "api", true)  
