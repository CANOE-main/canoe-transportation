"""Compatibility entrypoint for :mod:`parameterization.road_stocks_and_demands`.

New callers should import the road-specific module. This forwarding surface retains the
pre-alignment import and ``python -m`` command without preserving a second implementation.
"""

from parameterization.road_stocks_and_demands import *  # noqa: F403
from parameterization.road_stocks_and_demands import main


if __name__ == "__main__":
    main()
