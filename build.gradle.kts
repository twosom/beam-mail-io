plugins {
    java
    id("com.diffplug.spotless") version "6.2.0"
}

val beamVersion: String = "2.60.0"

repositories {
    mavenCentral()
}

allprojects {
    group = "com.icloud"
    version = "1.0.0-SNAPSHOT"

    plugins.apply {
        apply("java")
        apply("com.diffplug.spotless")
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    spotless {
        java {
            importOrder()
            removeUnusedImports()
            googleJavaFormat()
        }
    }

    repositories {
        mavenCentral()
    }

    dependencies {
        // beam bom
        implementation(platform("org.apache.beam:beam-sdks-java-google-cloud-platform-bom:$beamVersion"))

        beamImplementation(
            "beam-sdks-java-core",
        )

        beamRuntimeOnly(
            "beam-runners-direct-java",
            "beam-runners-flink-1.18"
        )

        // logger
        implementation("org.slf4j:slf4j-jdk14:1.7.32")
        implementation("ch.qos.logback:logback-classic:1.4.12")

        testImplementation(platform("org.junit:junit-bom:5.10.0"))
        testImplementation("org.junit.jupiter:junit-jupiter")
    }

    tasks.test {
        useJUnitPlatform()
    }
}


fun DependencyHandlerScope.beamImplementation(vararg args: String) {
    for (arg in args) {
        implementation("org.apache.beam:$arg")
    }
}

fun DependencyHandlerScope.beamRuntimeOnly(vararg args: String) {
    for (arg in args) {
        runtimeOnly("org.apache.beam:$arg")
    }
}