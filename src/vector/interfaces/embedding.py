"""Define the dense and sparse encoder operations used by the pipeline."""

from collections.abc import Sequence
from typing import Protocol

import numpy as np
from numpy.typing import NDArray

from vector.domain.models import SparseWeights


class DenseEncoder[Inputs](Protocol):
    def tokenize(self, texts: Sequence[str]) -> Inputs:
        """Prepare model inputs for a batch of texts."""
        ...

    def forward(self, inputs: Inputs) -> NDArray[np.float16]:
        """Encode prepared inputs as a float16 matrix, one row per text."""
        ...


class SparseEncoder(Protocol):
    def encode(self, texts: Sequence[str]) -> tuple[SparseWeights, ...]:
        """Return sparse token weights for each input text."""
        ...
