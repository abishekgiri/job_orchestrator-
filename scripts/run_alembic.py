#!/usr/bin/env python3
import sys
import os

# Append .lib directory to sys.path
sys.path.append(os.path.join(os.getcwd(), '.lib'))

# Append current directory to sys.path (for app imports)
sys.path.append(os.getcwd())

from alembic.config import main

if __name__ == '__main__':
    sys.exit(main())
