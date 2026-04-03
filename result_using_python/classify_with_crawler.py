#!/usr/bin/env python3
"""Thin entrypoint for the non-AI crawler."""

from crawler.app import main


if __name__ == "__main__":
    raise SystemExit(main())
