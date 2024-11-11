description = "Mail Connector for Apache Beam / Google Cloud Dataflow"

dependencies {
    implementation("com.sun.mail:jakarta.mail:2.0.1")
    // auto value
    compileOnly("com.google.auto.value:auto-value-annotations")
    annotationProcessor("com.google.auto.value:auto-value:1.10.4")
}