"""The timestamp stamped on webcam frames (lib/webcam/processing.py).

BC keeps UTC-7 all year from 2026-03-08. pytz's bundled tz data didn't know,
so from 2026-11-01 every frame was stamped an hour early ("14:21PST" on a
frame fetched at 15:21) and the cams looked an hour more delayed than they
were (user, 2026-11-06). zoneinfo reads the system's data; these pin it.
"""

from datetime import datetime, timezone

from lib.webcam.processing import retrieval_label


def test_after_bcs_switch_a_frame_is_stamped_utc_minus_7_as_pdt():
    fetched = datetime(2026, 11, 6, 22, 21, tzinfo=timezone.utc)
    assert retrieval_label(fetched) == "Retrieval time: 2026-11-06T15:21PDT"


def test_winter_after_the_switch_stays_utc_minus_7():
    assert retrieval_label(datetime(2027, 1, 15, 20, 5, tzinfo=timezone.utc)).endswith("T13:05PDT")


def test_the_last_winter_with_a_clock_change_was_pst():
    assert retrieval_label(datetime(2026, 1, 15, 20, 5, tzinfo=timezone.utc)).endswith("T12:05PST")
