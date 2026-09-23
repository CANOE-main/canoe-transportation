"""Compatibility entrypoint for :mod:`parameterization.road_lifetimes_survival`.

New callers should import the road-specific module. This forwarding surface retains the
pre-alignment import and ``python -m`` command without preserving a second implementation.
"""

from parameterization.road_lifetimes_survival import *  # noqa: F403
from parameterization.road_lifetimes_survival import main


if __name__ == "__main__":
    main()
