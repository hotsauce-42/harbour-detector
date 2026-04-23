FROM python:3.12-slim

# libgdal-dev: runtime GDAL libraries required by geopandas/fiona
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgdal-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first so this layer is cached independently
# of application code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

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
