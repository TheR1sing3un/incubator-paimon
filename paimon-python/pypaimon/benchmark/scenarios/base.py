################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################

import random
from abc import ABC, abstractmethod
from typing import Callable, Tuple

from pypaimon.benchmark.config import BenchmarkConfig


class BaseBenchmarkScenario(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def setup(self, config: BenchmarkConfig) -> dict:
        """Driver-side setup. Return context dict passed to every task."""
        pass

    @abstractmethod
    def make_request(self, catalog, context: dict,
                     rng: random.Random) -> Tuple[str, Callable[[], None]]:
        """Build one request. Return (api_name, thunk).
        The thunk is called inside the task and should perform the actual API call.
        """
        pass

    @abstractmethod
    def teardown(self, config: BenchmarkConfig, context: dict):
        """Cleanup created resources."""
        pass
