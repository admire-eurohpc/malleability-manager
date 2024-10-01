from abc import ABC, abstractmethod


class MalleabilityAlgorithm(ABC):

    @abstractmethod
    def initialize_algorithm(self, message: dict[str, str], **kwargs) -> dict | None:
        pass

    @abstractmethod
    def schedule(self, job_id: str, num_available_nodes: int) -> None or tuple[int, int]:
        pass
