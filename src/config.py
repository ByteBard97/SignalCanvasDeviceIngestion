"""Pipeline configuration from environment and defaults."""

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Pipeline configuration."""

    # Paths
    repo_root: Path = Path(__file__).parent.parent
    output_dir: Path = Path("output")
    stdlib_output: Path = Path("output/stdlib/devices")
    manifests_db: Path = Path("output/ingestion.db")
    pdf_cache_dir: Path = Path("output/pdf_cache")

    # Ragscallion RAG microservice
    ragscallion_host: str = "192.168.0.200"
    ragscallion_port: int = 8086
    ragscallion_ssh_user: str = "geoff"
    ragscallion_ssh_host: str = "192.168.0.200"
    ragscallion_script_path: str = "~/projects/device-library-rag/scripts/add-paper.sh"

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
    # alongside the primary spec_sheet. Disable in batch runs to cut Kimi cost.
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


# Singleton instance
settings = Settings()
