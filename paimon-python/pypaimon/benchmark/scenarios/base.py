import random
from abc import ABC, abstractmethod

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.metrics import MetricsCollector


class BaseBenchmarkScenario(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def setup(self, config: BenchmarkConfig) -> dict:
        """Create databases/tables/data needed. Return context dict passed to actors."""
        pass

    @abstractmethod
    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        """Execute one benchmark request and record metrics."""
        pass

    @abstractmethod
    def teardown(self, config: BenchmarkConfig, context: dict):
        """Cleanup created resources."""
        pass
