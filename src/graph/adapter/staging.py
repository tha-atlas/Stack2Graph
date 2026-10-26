"""Stage immutable index inputs by hard link, with copying as a fallback."""

import errno
import os
from pathlib import Path
from shutil import copyfileobj

from graph.domain.artifacts import ArtifactLayout

_LINK_UNAVAILABLE = frozenset({errno.EXDEV, errno.EPERM, errno.EACCES, errno.ENOSYS, errno.EOPNOTSUPP, errno.EMLINK})


class IndexInputStager:
    """
    Manage a staging directory while keeping published input files immutable.

    Hard links keep inputs available after their original paths are removed.
    Both paths share an inode, so producers and indexers must never edit a
    published shard in place. The index adapter copies metadata independently.
    """

    def __init__(self, root: Path) -> None:
        """Resolve and create the index-input staging root."""
        self.layout = ArtifactLayout(root.resolve())
        self.layout.root.mkdir(parents=True, exist_ok=True)

    def stage_file(self, source: Path, relative: str) -> Path:
        """Stage a regular file by hard link or copy without replacing output."""
        if source.is_symlink() or not source.is_file():
            raise ValueError("Index input must be a regular published file")
        destination = self.layout.path(relative)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(source, destination, follow_symlinks=False)
        except OSError as error:
            if error.errno not in _LINK_UNAVAILABLE:
                raise
            created = False
            try:
                with destination.open("xb") as output:
                    created = True
                    with source.open("rb") as input_stream:
                        copyfileobj(input_stream, output, length=1024**2)
            except BaseException:
                if created:
                    destination.unlink(missing_ok=True)
                raise
        return destination
