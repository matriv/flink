/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-core"))
    implementation(project(":flink-runtime"))
    implementation(project(":flink-streaming-java"))
    implementation("org.apache.flink:flink-shaded-guava:30.1.1-jre-14.0")
    testImplementation(project(":flink-runtime"))
    testImplementation(project(":flink-streaming-java"))
    testImplementation(project(":flink-test-utils-junit"))
}

description = "Flink : DSTL : DFS"
