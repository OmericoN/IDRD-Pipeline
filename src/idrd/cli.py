"""Compatibility entrypoint for the CLI interface."""

from idrd.interfaces.cli.main import IDRDPipeline, build_parser, main

__all__ = ["IDRDPipeline", "build_parser", "main"]
