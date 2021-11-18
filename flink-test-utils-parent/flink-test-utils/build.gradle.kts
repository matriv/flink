/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-core"))
    implementation(project(":flink-test-utils-junit"))
    implementation(project(":flink-runtime"))
    implementation(project(":flink-runtime", "testArtifacts"))
    implementation(project(":flink-rpc-akka-loader"))
    implementation(project(":flink-clients"))
    implementation(project(":flink-streaming-java"))
    implementation("org.apache.commons:commons-lang3:3.3.2")
    implementation("org.apache.curator:curator-test:2.12.0")
    implementation("org.apache.hadoop:hadoop-minikdc:3.2.0")
    implementation("org.apache.flink:flink-shaded-netty:4.1.65.Final-14.0")
    runtimeOnly(project(":flink-statebackend-changelog"))
}

description = "Flink : Test utils : Utils"
