#!/usr/bin/env python3
"""KaKaBot launcher."""
import sys
import os

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_lean_bot.main import main

if __name__ == "__main__":
    main()
