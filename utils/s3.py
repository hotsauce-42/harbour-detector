"""
S3 configuration and file-system utilities.

Credential resolution order (first non-empty value wins):
  1. settings.yaml  [s3]  section  (highest priority)
  2. Environment variables
  3. .env file (loaded only for keys not already set in the environment)

For MinIO or other S3-compatible storage, set endpoint_url.
"""

import os
from pathlib import Path

_DOTENV_LOADED = False


def _load_dotenv() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(override=False)  # never overwrite vars already in the environment
    except ImportError:
        pass
    _DOTENV_LOADED = True


def build_s3_config(yaml_s3: dict | None = None) -> dict:
    """
    Merge the [s3] section from settings.yaml with environment variables.
    yaml_s3 values take precedence; env vars are the fallback.

    Returns a dict with keys: key, secret, region, endpoint_url.
    Values are empty strings when not configured anywhere.
    """
    _load_dotenv()
    yaml_s3 = yaml_s3 or {}

    def _pick(yaml_key: str, env_key: str) -> str:
        v = yaml_s3.get(yaml_key) or ""
        return str(v).strip() if str(v).strip() else (os.environ.get(env_key) or "").strip()

    return {
        "key":          _pick("access_key_id",    "AWS_ACCESS_KEY_ID"),
        "secret":       _pick("secret_access_key", "AWS_SECRET_ACCESS_KEY"),
        "region":       _pick("region",            "AWS_DEFAULT_REGION") or "us-east-1",
        "endpoint_url": _pick("endpoint_url",      "S3_ENDPOINT_URL"),
    }


def is_s3_path(path: str | None) -> bool:
    """Return True if path is an S3 URI (starts with 's3://')."""
    return bool(path) and str(path).startswith("s3://")


def path_join(base: str, *parts: str) -> str:
    """
    Join path components, working correctly for both local paths and S3 URIs.

    pathlib.Path collapses 's3://bucket' → 's3:/bucket', so S3 paths
    must be joined with plain string operations.
    """
    if is_s3_path(base):
        result = base.rstrip("/")
        for p in parts:
            part = str(p).strip("/")
            if part:
                result = result + "/" + part
        return result
    return str(Path(base).joinpath(*parts))


def ensure_dir(path: str) -> None:
    """Create local directory tree if needed. No-op for S3 paths (S3 has no directories)."""
    if not is_s3_path(path):
        Path(path).mkdir(parents=True, exist_ok=True)


def get_s3_filesystem(s3_cfg: dict):
    """
    Return an s3fs.S3FileSystem configured with the given credentials.

    Use the returned object for binary writes:
        with get_s3_filesystem(cfg).open("s3://bucket/key", "wb") as fh:
            pq.write_table(table, fh, compression="snappy")
    """
    import s3fs  # type: ignore[import]

    kwargs: dict = {}
    if s3_cfg.get("key"):
        kwargs["key"] = s3_cfg["key"]
    if s3_cfg.get("secret"):
        kwargs["secret"] = s3_cfg["secret"]
    if s3_cfg.get("endpoint_url"):
        kwargs["endpoint_url"] = s3_cfg["endpoint_url"]

    client_kwargs: dict = {}
    if s3_cfg.get("region"):
        client_kwargs["region_name"] = s3_cfg["region"]
    if client_kwargs:
        kwargs["client_kwargs"] = client_kwargs

    return s3fs.S3FileSystem(**kwargs)


def get_s3_storage_options(s3_cfg: dict) -> dict:
    """
    Return a storage_options dict for pd.read_parquet(storage_options=...).
    """
    opts: dict = {}
    if s3_cfg.get("key"):
        opts["key"] = s3_cfg["key"]
    if s3_cfg.get("secret"):
        opts["secret"] = s3_cfg["secret"]

    client_kwargs: dict = {}
    if s3_cfg.get("endpoint_url"):
        client_kwargs["endpoint_url"] = s3_cfg["endpoint_url"]
    if s3_cfg.get("region"):
        client_kwargs["region_name"] = s3_cfg["region"]
    if client_kwargs:
        opts["client_kwargs"] = client_kwargs

    return opts


def configure_duckdb_s3(con, s3_cfg: dict) -> None:
    """
    Load the httpfs extension and configure S3 credentials on a DuckDB connection.
    Must be called before any read_parquet('s3://...') query on that connection.

    MinIO note: sets s3_url_style='path' and s3_use_ssl when endpoint_url is present.
    """
    con.execute("INSTALL httpfs IF NOT EXISTS; LOAD httpfs;")

    if s3_cfg.get("key"):
        con.execute(f"SET s3_access_key_id='{_esc(s3_cfg['key'])}';")
    if s3_cfg.get("secret"):
        con.execute(f"SET s3_secret_access_key='{_esc(s3_cfg['secret'])}';")
    if s3_cfg.get("region"):
        con.execute(f"SET s3_region='{_esc(s3_cfg['region'])}';")

    if s3_cfg.get("endpoint_url"):
        endpoint = s3_cfg["endpoint_url"]
        use_ssl  = endpoint.startswith("https://")
        host     = endpoint.replace("https://", "").replace("http://", "").rstrip("/")
        con.execute(f"SET s3_endpoint='{_esc(host)}';")
        con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
        con.execute("SET s3_url_style='path';")  # MinIO requires path-style addressing


def _esc(value: str) -> str:
    """Escape single quotes in a SQL string literal."""
    return value.replace("'", "''")
