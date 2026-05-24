"""Pipeline configuration from environment and defaults."""

import socket
from pathlib import Path

from pydantic_settings import BaseSettings


def _discover_ragscallion(configured_host: str, port: int, timeout: float = 1.0) -> str:
    """Return the first reachable Ragscallion host.

    Tries the configured host first; if unreachable, scans the /24 subnet
    of the default gateway interface for a host with the port open.
    """
    def _reachable(host: str) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False

    if _reachable(configured_host):
        return configured_host

    # Derive local /24 subnet from hostname resolution of the local machine
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
        prefix = ".".join(local_ip.split(".")[:3])
        for i in range(1, 255):
            candidate = f"{prefix}.{i}"
            if candidate != local_ip and _reachable(candidate):
                return candidate
    except OSError:
        pass

    return configured_host  # fall back; will fail at connection time with a clear error


class Settings(BaseSettings):
    """Pipeline configuration."""

    # Paths
    repo_root: Path = Path(__file__).parent.parent
    output_dir: Path = Path("output")
    stdlib_output: Path = Path("output/stdlib/devices")
    manifests_db: Path = Path("output/ingestion.db")
    pdf_cache_dir: Path = Path("output/pdf_cache")

    # Ragscallion RAG microservice (https://github.com/ByteBard97/ragscallion)
    ragscallion_host: str = "localhost"
    ragscallion_port: int = 8086
    ragscallion_ssh_user: str = "your-username"
    ragscallion_ssh_host: str = "localhost"
    ragscallion_script_path: str = "~/projects/ragscallion/scripts/add-paper.sh"

    # API keys
    claude_api_key: str = ""  # Set from environment
    moonshot_api_key: str = ""  # Set from environment

    # Ingestion phases
    phase_0_device_count: int = 3  # Validation phase
    phase_1_device_count: int = 50  # Test harness phase
    phase_2_device_count: int = 1500  # Mid-tier devices
    phase_3_remaining: bool = True  # Process remaining devices

    # Extraction
    extraction_timeout_seconds: int = 120
    extraction_max_retries: int = 3

    # PDF processing
    pdf_download_timeout_seconds: int = 60
    marker_timeout_seconds: int = 300
    marker_max_memory_gb: int = 8

    # Multi-doc discovery: also search for user_manual and install_guide
    # alongside the primary spec_sheet. Each device gets 2 extra Kimi searches.
    find_secondary_docs: bool = True

    class Config:
        # Use repo-relative path so .env is found regardless of CWD
        env_file = str(Path(__file__).parent.parent / ".env")
        env_file_encoding = "utf-8"
        extra = "ignore"

    def ensure_output_dirs(self):
        """Create all required output directories."""
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.stdlib_output.mkdir(exist_ok=True, parents=True)
        self.pdf_cache_dir.mkdir(exist_ok=True, parents=True)

    def ragscallion_base_url(self) -> str:
        host = _discover_ragscallion(self.ragscallion_host, self.ragscallion_port)
        return f"http://{host}:{self.ragscallion_port}"

    def ragscallion_search_url(self) -> str:
        return f"{self.ragscallion_base_url()}/search"


# Singleton instance
settings = Settings()
