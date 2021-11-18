/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-core"))
    implementation(project(":flink-runtime"))
    implementation(project(":flink-runtime", "testArtifacts"))
    implementation(project(":flink-streaming-java"))
    implementation(project(":flink-test-utils"))
    implementation(project(":flink-clients"))
    implementation("org.testcontainers:testcontainers:1.16.2")
    implementation(libs.junit.jupiter)
    implementation(libs.junit.vintage)
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.log4j.slf4j)
}

description = "Flink : Test utils : Testing Framework"
