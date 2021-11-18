/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

val testArtifacts: Configuration by configurations.creating

dependencies {
    api(project(":flink-java"))
    implementation(project(":flink-core"))
    implementation(project(":flink-file-sink-common"))
    implementation(project(":flink-runtime"))
    implementation("commons-io:commons-io:2.11.0")
    implementation("org.apache.commons:commons-lang3:3.3.2")
    implementation("org.apache.flink:flink-shaded-guava:30.1.1-jre-14.0")
    implementation("org.apache.flink:flink-shaded-jackson:2.12.4-14.0")
    implementation("org.apache.commons:commons-math3:3.6.1")
    implementation("com.esotericsoftware.kryo:kryo:2.24.0")
    testImplementation(project(":flink-core", "testArtifacts"))
    testImplementation(project(":flink-test-utils-junit"))
    testImplementation(project(":flink-runtime", "testArtifacts"))
    testImplementation("org.apache.flink:flink-shaded-netty:4.1.65.Final-14.0")
}

description = "Flink : Streaming Java"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

artifacts {
    add("testArtifacts", testsJar)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
