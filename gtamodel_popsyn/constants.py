#PUMA_PD_RANGES = [range(1, 12), range(12, 24), range(24, 36), range(36, 100000)]
PUMA_PD_RANGES = [range(1,500)]

ZONE_RANGE = range(1, 6000)


# Capture 0 for non workers
INTERNAL_ZONE_RANGE = range(1, 6000)
EXTERNAL_ZONE_RANGE = range(6000, 8887)
ROAMING_ZONE_ID = 8888

"""
Age bins
"""
AGE_BINS = [range(0, 4), range(5, 10), range(11, 15), range(16, 25), range(26, 35), range(36, 45), range(46, 55),
            range(56, 64),
            range(65, 200)]
