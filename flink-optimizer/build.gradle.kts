/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

val testArtifacts: Configuration by configurations.creating

dependencies {
    implementation(project(":flink-core"))
    implementation(project(":flink-runtime"))
    implementation(project(":flink-java"))
    implementation("org.apache.commons:commons-lang3:3.3.2")
    implementation("org.apache.flink:flink-shaded-guava:30.1.1-jre-14.0")
    implementation("org.apache.flink:flink-shaded-jackson:2.12.4-14.0")
    testImplementation(project(":flink-test-utils-junit"))
    testImplementation(project(":flink-runtime"))
}

description = "Flink : Optimizer"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)

artifacts {
    add("testArtifacts", testsJar)
}
