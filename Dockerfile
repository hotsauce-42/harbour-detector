FROM python:3.12-slim

# libgdal-dev: geopandas/fiona runtime; default-jdk-headless: PySpark JVM
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    libgdal-dev \
    default-jdk-headless \
 && rm -rf /var/lib/apt/lists/*

# Point PySpark at the JVM installed above
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

# Install Python dependencies first so this layer is cached independently
# of application code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download S3A connector JARs into PySpark's own jars/ directory so no
# Maven / internet access is needed at runtime.
# hadoop-aws version must match the Hadoop bundled with pyspark 4.0.0 (3.3.4).
# aws-java-sdk-bundle 1.12.262 is the compatible AWS SDK for hadoop-aws 3.3.4.
RUN python3 -c "\
import urllib.request, os, pyspark; \
d = os.path.join(os.path.dirname(pyspark.__file__), 'jars'); \
urllib.request.urlretrieve('https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar', os.path.join(d, 'hadoop-aws-3.3.4.jar')); \
urllib.request.urlretrieve('https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar', os.path.join(d, 'aws-java-sdk-bundle-1.12.262.jar')); \
print('S3A JARs downloaded to', d) \
"

# Pre-warm the reverse_geocoder GeoNames dataset so the image never needs
# outbound internet access at runtime.
RUN python -c "import reverse_geocoder" 2>/dev/null || true

# Application code and default configuration (baked into the image).
# Individual settings are overridable at runtime via environment variables;
# see run_pipeline.py for the full list.
COPY config/        config/
COPY models/        models/
COPY pipeline/      pipeline/
COPY utils/         utils/
COPY run.py         run.py
COPY run_pipeline.py run_pipeline.py

ENTRYPOINT ["python", "run_pipeline.py"]
