"""Dataproc Serverless entry point. Ship the ridespark package next to it with --py-files."""
import sys

from ridespark.job import main

if __name__ == "__main__":
    sys.exit(main())
