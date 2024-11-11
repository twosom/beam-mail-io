plugins {
    java
    id("com.diffplug.spotless") version "6.2.0"
}

group = "com.icloud"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

spotless {
    java {
        importOrder()
        removeUnusedImports()
        googleJavaFormat()
    }
}

tasks.test {
    useJUnitPlatform()
}