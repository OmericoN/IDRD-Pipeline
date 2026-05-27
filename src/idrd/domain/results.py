"""
Result dataclasses for pipeline operations.

These dataclasses represent the outcomes of pipeline operations (download, conversion, rendering)
without coupling to any specific storage backend. Components return these results, and the caller
decides how to persist them (database, DataFrame, JSON, etc.).
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from datetime import datetime


@dataclass
class DownloadResult:
    """Result of a PDF download operation."""
    
    paper_id: str
    success: bool
    message: str
    filepath: Optional[Path] = None
    error: Optional[str] = None
    file_size_bytes: int = 0
    download_time: Optional[datetime] = None
    url: Optional[str] = None
    
    def __post_init__(self):
        """Set download_time if not provided."""
        if self.download_time is None:
            self.download_time = datetime.now()
    
    @property
    def file_size_mb(self) -> float:
        """Return file size in megabytes."""
        return self.file_size_bytes / (1024 * 1024) if self.file_size_bytes > 0 else 0.0


@dataclass
class ConversionResult:
    """Result of a PDF to XML conversion operation."""
    
    paper_id: str
    success: bool
    message: str
    xml_path: Optional[Path] = None
    pdf_path: Optional[Path] = None
    error: Optional[str] = None
    conversion_time: Optional[datetime] = None
    xml_size_bytes: int = 0
    
    def __post_init__(self):
        """Set conversion_time if not provided."""
        if self.conversion_time is None:
            self.conversion_time = datetime.now()
    
    @property
    def xml_size_kb(self) -> float:
        """Return XML file size in kilobytes."""
        return self.xml_size_bytes / 1024 if self.xml_size_bytes > 0 else 0.0


@dataclass
class RenderResult:
    """Result of a TEI XML to Markdown rendering operation."""
    
    paper_id: str
    xml_path: Path
    md_path: Path
    success: bool
    message: str
    error: Optional[str] = None
    render_time: Optional[datetime] = None
    sections_extracted: int = 0
    references_count: int = 0
    
    def __post_init__(self):
        """Set render_time if not provided."""
        if self.render_time is None:
            self.render_time = datetime.now()


@dataclass
class PipelineStats:
    """Aggregate statistics for a batch operation."""
    
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    total_size_bytes: int = 0
    
    @property
    def total(self) -> int:
        """Total operations attempted."""
        return self.successful + self.failed
    
    @property
    def success_rate(self) -> float:
        """Success rate as a percentage."""
        return (self.successful / self.total * 100) if self.total > 0 else 0.0
    
    @property
    def total_size_mb(self) -> float:
        """Total size in megabytes."""
        return self.total_size_bytes / (1024 * 1024)
    
    @property
    def avg_size_mb(self) -> float:
        """Average size per successful operation in megabytes."""
        return (self.total_size_bytes / self.successful / (1024 * 1024)) if self.successful > 0 else 0.0
