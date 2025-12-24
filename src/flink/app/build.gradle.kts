plugins {
    java
    application
    id("com.gradleup.shadow") version "8.3.5"
}

group = "org.streaming"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.flink:flink-streaming-java:1.19.0")
    compileOnly("org.apache.flink:flink-clients:1.19.0")

    compileOnly("org.apache.flink:flink-streaming-scala_2.12:1.19.0")
    compileOnly("org.scala-lang:scala-library:2.12.18")

    implementation("org.apache.flink:flink-connector-kafka:3.3.0-1.19")
    implementation("org.apache.flink:flink-connector-cassandra_2.12:3.2.0-1.19")
    implementation("com.starrocks:flink-connector-starrocks:1.2.12_flink-1.19")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
}
application {
    mainClass.set("org.streaming.Application")
}

// Configure ShadowJar for Kotlin DSL
tasks.shadowJar {
    archiveClassifier.set("") 
    mergeServiceFiles()       
    
    // Fix for potential "Zip file duplicate" errors in some Flink/Jackson setups
    append("META-INF/spring.handlers")
    append("META-INF/spring.schemas")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}