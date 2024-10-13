import hashlib
import os
import shutil
from pathlib import Path
from typing import Protocol


class LocalFileSystem:
    def __init__(self):
        self.actions = []

    def read(self, path):
        return read_paths_and_hashes(path)

    def copy(self, source, dest):
        shutil.copyfile(source, dest)
        self.actions.append(("COPY", source, dest))

    def move(self, source, dest):
        shutil.move(source, dest)
        self.actions.append(("MOVE", source, dest))

    def delete(self, dest):
        os.remove(dest)
        self.actions.append(("DELETE", dest))

    def get_actions(self):
        return self.actions


class FakeFileSystem:
    def __init__(self, path_hashes):
        self.path_hashes = path_hashes
        self.actions = []

    def read(self, path):
        return self.path_hashes[path]

    def copy(self, source, dest):
        self.actions.append(("COPY", source, dest))

    def move(self, source, dest):
        self.actions.append(("MOVE", source, dest))

    def delete(self, dest):
        self.actions.append(("DELETE", dest))

    def get_actions(self) -> list[tuple[str, Path, Path] | tuple[str, Path]]:
        return self.actions


class BaseFileSystem(Protocol):
    def read(self, path: Path) -> dict[str, Path]:
        pass

    def copy(self, source: Path, dest: Path):
        pass

    def move(self, source: Path, dest: Path):
        pass

    def delete(self, dest: Path):
        pass

    def get_actions(self) -> list[tuple[str, Path, Path] | tuple[str, Path]]:
        pass


def sync(source, dest, filesystem: BaseFileSystem):
    # imperative shell step 1, gather inputs
    source_hashes = filesystem.read(path=source)
    dest_hashes = filesystem.read(path=dest)

    for sha, filename in source_hashes.items():
        if sha not in dest_hashes:
            sourcepath = Path(source) / filename
            destpath = Path(dest) / filename
            filesystem.copy(sourcepath, destpath)  # (3)

        elif dest_hashes[sha] != filename:
            olddestpath = Path(dest) / dest_hashes[sha]
            newdestpath = Path(dest) / filename
            filesystem.move(olddestpath, newdestpath)  # (3)

    for sha, filename in dest_hashes.items():
        if sha not in source_hashes:
            filesystem.delete(dest / filename)  # (3)


BLOCKSIZE = 65536


def hash_file(path) -> str:
    hasher = hashlib.sha1()
    with path.open("rb") as file:
        buf = file.read(BLOCKSIZE)
        while buf:
            hasher.update(buf)
            buf = file.read(BLOCKSIZE)
    return hasher.hexdigest()


def read_paths_and_hashes(root):
    hashes = {}
    for folder, _, files in os.walk(root):
        for fn in files:
            hashes[hash_file(Path(folder) / fn)] = fn
    return hashes


def determine_actions(source_hashes, dest_hashes, source_folder, dest_folder):
    for sha, filename in source_hashes.items():
        if sha not in dest_hashes:
            sourcepath = Path(source_folder) / filename
            destpath = Path(dest_folder) / filename
            yield "COPY", sourcepath, destpath

        elif dest_hashes[sha] != filename:
            olddestpath = Path(dest_folder) / dest_hashes[sha]
            newdestpath = Path(dest_folder) / filename
            yield "MOVE", olddestpath, newdestpath

    for sha, filename in dest_hashes.items():
        if sha not in source_hashes:
            yield "DELETE", dest_folder / filename


def determine_actions_2(source_hashes, dest_hashes, source_folder, dest_folder):
    actions = []
    for sha, filename in source_hashes.items():
        if sha not in dest_hashes:
            sourcepath = Path(source_folder) / filename
            destpath = Path(dest_folder) / filename
            actions.append(("COPY", sourcepath, destpath))
            # yield "COPY", sourcepath, destpath

        elif dest_hashes[sha] != filename:
            olddestpath = Path(dest_folder) / dest_hashes[sha]
            newdestpath = Path(dest_folder) / filename
            actions.append(("MOVE", olddestpath, newdestpath))
            # yield "MOVE", olddestpath, newdestpath

    for sha, filename in dest_hashes.items():
        if sha not in source_hashes:
            # yield "DELETE", dest_folder / filename
            actions.append(("DELETE", dest_folder / filename))
    return actions
