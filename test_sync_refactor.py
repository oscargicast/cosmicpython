from sync_refactor import FakeFileSystem, sync
from pathlib import Path


def test_when_a_single_file_exists_in_the_source_but_not_the_destination():
    fakefs = FakeFileSystem(
        {
            "/src": {
                "hash1": "fn1",
            },
            "/dst": {},
        }
    )
    sync("/src", "/dst", filesystem=fakefs)
    assert fakefs.get_actions() == [
        (
            "COPY",
            Path("/src/fn1"),
            Path("/dst/fn1"),
        ),
    ]


def test_when_a_file_exists_in_the_source_but_not_the_destination():
    fakefs = FakeFileSystem(
        {
            "/src": {
                "hash1": "fn1",
                "hash2": "fn2",
            },
            "/dst": {},
        }
    )
    sync("/src", "/dst", filesystem=fakefs)
    assert fakefs.get_actions() == [
        (
            "COPY",
            Path("/src/fn1"),
            Path("/dst/fn1"),
        ),
        (
            "COPY",
            Path("/src/fn2"),
            Path("/dst/fn2"),
        ),
    ]


def test_when_a_file_has_been_renamed_in_the_source():
    fakefs = FakeFileSystem(
        {
            "/src": {
                "hash1": "fn1",
            },
            "/dst": {
                "hash1": "fn2",
            },
        }
    )
    sync("/src", "/dst", filesystem=fakefs)
    assert fakefs.get_actions() == [
        (
            "MOVE",
            Path("/dst/fn2"),
            Path("/dst/fn1"),
        )
    ]
