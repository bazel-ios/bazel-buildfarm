"""
buildfarm definitions and configurations around choosing JVM flags for java images.
"""

SERVER_TELEMETRY_JVM_FLAGS = [
    "-javaagent:/app/build_buildfarm/opentelemetry-javaagent.jar",
    "-Dotel.resource.attributes=service.name=server",
    "-Dotel.exporter.otlp.traces.endpoint=http://otel-collector:4317",
    "-Dotel.instrumentation.http.capture-headers.client.request",
    "-Dotel.instrumentation.http.capture-headers.client.response",
    "-Dotel.instrumentation.http.capture-headers.server.request",
    "-Dotel.instrumentation.http.capture-headers.server.response",
]

WORKER_TELEMETRY_JVM_FLAGS = [
    "-javaagent:/app/build_buildfarm/opentelemetry-javaagent.jar",
    "-Dotel.resource.attributes=service.name=worker",
    "-Dotel.exporter.otlp.traces.endpoint=http://otel-collector:4317",
    "-Dotel.instrumentation.http.capture-headers.client.request",
    "-Dotel.instrumentation.http.capture-headers.client.response",
    "-Dotel.instrumentation.http.capture-headers.server.request",
    "-Dotel.instrumentation.http.capture-headers.server.response",
]

RECOMMENDED_JVM_FLAGS = [
    # Enables the JVM to detect if it is running inside a container and automatically adjusts
    # its behavior to optimize performance. This flag can help ensure that the JVM is
    # configured optimally for containerized environments.
    "-XX:+UseContainerSupport",

    # Enables the deduplication of identical strings in the JVM's string pool,
    # which can help reduce memory usage.
    "-XX:+UseStringDeduplication",

    # Do not dump the heap or exit on OOM
    # It may cause no disk space issue or too many restarts of the container.
    # "-XX:+HeapDumpOnOutOfMemoryError",
]

SERVER_MEMORY_JVM_FLAGS = [
    "-XX:MaxRAMPercentage=80.0",

    # This flag enables compressed object pointers in the JVM,
    # which can reduce the memory footprint of objects on system.
    "-XX:+UseCompressedOops",
]

WORKER_MEMORY_JVM_FLAGS = [
    # Sized for the in-heap CAS index, not the host. UseCompressedOops is
    # omitted deliberately: it clamps the heap to ~32 GiB.
    "-XX:MaxRAMPercentage=20.0",
]

DEFAULT_LOGGING_CONFIG = ["-Dlogging.config=file:/app/build_buildfarm/src/main/java/build/buildfarm/logging.properties"]

def ensure_accurate_metadata():
    return select({
        "//conditions:default": [],
        "//config:windows": ["-Dsun.nio.fs.ensureAccurateMetadata=true"],
    })

def add_opens_sun_nio_fs():
    return select({
        "//conditions:default": [],
        "//config:windows": ["--add-opens java.base/sun.nio.fs=ALL-UNNAMED"],
    })

def server_telemetry():
    return select({
        "//config:open_telemetry": SERVER_TELEMETRY_JVM_FLAGS,
        "//conditions:default": [],
    })

def worker_telemetry():
    return select({
        "//config:open_telemetry": WORKER_TELEMETRY_JVM_FLAGS,
        "//conditions:default": [],
    })

def server_jvm_flags():
    return RECOMMENDED_JVM_FLAGS + SERVER_MEMORY_JVM_FLAGS + DEFAULT_LOGGING_CONFIG + ensure_accurate_metadata() + add_opens_sun_nio_fs() + server_telemetry()

def worker_jvm_flags():
    return RECOMMENDED_JVM_FLAGS + WORKER_MEMORY_JVM_FLAGS + DEFAULT_LOGGING_CONFIG + ensure_accurate_metadata() + add_opens_sun_nio_fs() + worker_telemetry()
