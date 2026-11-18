"""Disk-backed float64 mobility profiles for the annual Ontario rerun.

Only storage changes: rows remain local-clock minutes and columns remain the
original user order. No random generator is used here.
"""

import operator
from pathlib import Path

import numpy as np


MINUTES_PER_DAY = 1440


def create_profile_store(directory, users, num_days):
    """Create one numbered .npy map per nonempty class; never replace a file.

    Empty classes have a zero-column ndarray and require no backing storage.
    Fortran order keeps each individual driver's year contiguous for charging.
    """
    days = operator.index(num_days)
    if days <= 0:
        raise ValueError("Number of stored mobility days must be positive")
    counts = [(user.user_name, operator.index(user.num_users)) for user in users]
    if len({name for name, _ in counts}) != len(counts):
        raise ValueError("Mobility user class names must be unique")
    if any(count < 0 for _, count in counts):
        raise ValueError("Mobility user counts must be nonnegative")
    directory = Path(directory)
    paths = [directory / f"user-{index:03d}.npy" for index in range(len(counts))]
    for path in paths:
        if path.exists():
            raise FileExistsError(f"Refusing to replace mobility scratch data: {path}")
    directory.mkdir(parents=True, exist_ok=True)
    profiles = {}
    try:
        for (name, count), path in zip(counts, paths):
            shape = (days * MINUTES_PER_DAY, count)
            if count == 0:
                profiles[name] = np.empty(shape, dtype=np.float64)
            else:
                profiles[name] = np.lib.format.open_memmap(
                    path, mode="w+", dtype=np.float64, shape=shape, fortran_order=True,
                )
    except BaseException:
        close_profile_store(profiles)
        raise
    return profiles


def write_profile_day(profiles, day_index, user_name, user_index, daily_profile):
    """Write one driver's daily profile at its exact original row/column."""
    day = operator.index(day_index)
    user = operator.index(user_index)
    target = profiles[user_name]
    start = day * MINUTES_PER_DAY
    end = start + MINUTES_PER_DAY
    if day < 0 or end > target.shape[0] or not 0 <= user < target.shape[1]:
        raise IndexError("Mobility profile day/user is outside its storage shape")
    values = np.asarray(daily_profile, dtype=np.float64)
    if values.shape != (MINUTES_PER_DAY,):
        raise ValueError("A daily mobility profile must contain 1440 minute values")
    target[start:end, user] = values


def flush_profile_store(profiles):
    for profile in profiles.values():
        if isinstance(profile, np.memmap):
            profile.flush()


def close_profile_store(profiles):
    """Flush and release Windows mappings after all borrowed views are released.

    Callers must not access the arrays after closing. Repeated cleanup is safe.
    """
    for profile in profiles.values():
        if isinstance(profile, np.memmap) and not profile._mmap.closed:
            profile.flush()
            profile._mmap.close()
