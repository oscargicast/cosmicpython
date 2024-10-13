import tempfile

from sync_refactor import LocalFileSystem, sync
from pathlib import Path


def test_when_a_single_file_exists_in_the_source_but_not_the_destination():
    source = tempfile.mkdtemp()
    dest = tempfile.mkdtemp()

    content = "I am a very useful file"
    (Path(source) / "fn1").write_text(content)

    fs = LocalFileSystem()
    sync(source, dest, filesystem=fs)
    assert fs.get_actions() == [
        (
            "COPY",
            Path(source) / "fn1",
            Path(dest) / "fn1",
        ),
    ]

    expected_fn1 = Path(dest) / "fn1"
    assert expected_fn1.exists()
    assert expected_fn1.read_text() == content
