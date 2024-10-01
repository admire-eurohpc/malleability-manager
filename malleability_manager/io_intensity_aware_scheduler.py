from malleability_manager import MalleabilityAlgorithm, MetricProxyAdapter


class IOIntensityAwareScheduler(MalleabilityAlgorithm):

    def __init__(self) -> None:
        self._metric_proxy: MetricProxyAdapter | None = None
        self._procs_per_node: int | None = None
        self._min_required_profiles: int | None = None
        self._efficiency_threshold: int | None = None
        self._workload_bw: float = 0.0
        self._system_bw: float = 0.0
        self._workload_io_intensity: float = 0.0
        self._system_io_intensity: float = 0.0
        self._initialized: bool = False

    def initialize_algorithm(self, message: dict[str, str], **kwargs) -> dict | None:
        if 'metric_proxy' not in kwargs:
            raise ValueError('Metric proxy adapter not passed to I/O-intensity-aware scheduler')
        self._metric_proxy = kwargs['metric_proxy']

        if 'command' not in message:
            return {'result': 'error', 'message': '"command" expected in message but is missing'}
        command = message['command']
        if command != 'init':
            return {'result': 'error', 'message': f'Expected "init" as initialization command but got {command}'}

        if 'procs_per_node' not in message:
            return {'result': 'error', 'message': '"procs_per_node" expected in message but is missing'}
        try:
            self._procs_per_node = int(message['procs_per_node'])
        except ValueError:
            value_type = type(message["procs_per_node"])
            return {'result': 'error', 'message': f'"procs_per_node" must be of type int but is {value_type}'}

        if 'min_required_profiles' not in message:
            return {'result': 'error', 'message': '"min_required_profiles" expected in message but is missing'}
        try:
            self._min_required_profiles = int(message['min_required_profiles'])
        except ValueError:
            value_type = type(message["min_required_profiles"])
            return {'result': 'error', 'message': f'"min_required_profiles" must be of type int but is {value_type}'}

        if 'efficiency_threshold' not in message:
            return {'result': 'error', 'message': '"efficiency_threshold" expected in message but is missing'}
        try:
            self._efficiency_threshold = float(message['efficiency_threshold'])
        except ValueError:
            value_type = type(message["efficiency_threshold"])
            return {'result': 'error', 'message': f'"efficiency_threshold" must be of type float but is {value_type}'}

        self._initialized = True

    def _calculate_io_intensities(self, job_id: str, start: float, end: float, step: float) -> list[float] | None:
        io_times = self._metric_proxy.get_model_for(job_id, 'mpi___time___mpi_wtime', start, end, step)
        if io_times is None:
            return None
        io_times = [float(value[1]) for value in io_times]
        total_times = self._metric_proxy.get_model_for(job_id, 'walltime', start, end, step)
        total_times = [float(value[1]) for value in total_times]
        if total_times is None:
            return None
        read_bytes_metric = 'proxy_network_receive_bytes_total{interface=\"ibs1\"}'
        read_bytes = self._metric_proxy.get_model_for(job_id, read_bytes_metric, start, end, step)
        read_bytes = [float(value[1]) for value in read_bytes]
        if read_bytes is None:
            return None
        written_bytes_metric = 'proxy_network_transmit_bytes_total{interface=\"ibs1\"}'
        written_bytes = self._metric_proxy.get_model_for(job_id, written_bytes_metric, start, end, step)
        written_bytes = [float(value[1]) for value in written_bytes]
        if written_bytes is None:
            return None
        fractions_io = []
        bandwidths = []
        io_intensities = []
        for i in range(len(io_times)):
            fractions_io.append(io_times[i] / total_times[i])
            bandwidths.append((read_bytes[i] + written_bytes[i]) / io_times[i])
            io_intensities.append(fractions_io[i] * bandwidths[i])
        return io_intensities

    def _calculate_io_intensity(self, job_id: str, size: int) -> float | None:
        result = self._calculate_io_intensities(job_id, size, size + 1, 1)
        return result[0] if result is not None else None

    def _update_io_intensities(self, pending_jobs: dict[str, dict], running_jobs: dict[str, dict],
                               triggering_job_id: str, profiles: list[dict]) -> dict | None:

        for _, job in pending_jobs.items():
            # ADMIRE workaround: retrieve command from Slurm comment
            command = job['COMMENT']
            if command == '(null)':
                continue
            located_job_id = None
            for profile in profiles:
                if profile['command'] == command:
                    located_job_id = profile['jobid']
                    break
            if located_job_id is None:
                continue
            # derive size from number of nodes and processes per node
            size = int(job['NODES']) * self._procs_per_node
            job_io_intensity = self._calculate_io_intensity(located_job_id, size)
            if job_io_intensity is None:
                continue
            self._workload_bw += job_io_intensity

        located_job = None
        for job_id, job in running_jobs.items():
            if job_id == triggering_job_id:
                located_job = job
            command = job['command']
            if command == '(null)':
                continue
            located_job_id = None
            for profile in profiles:
                if profile['command'] == command:
                    located_job_id = profile['jobid']
                    break
            if located_job_id is None:
                continue
            size = int(job['size'])
            job_io_intensity = self._calculate_io_intensity(located_job_id, size)
            if job_io_intensity is None:
                continue
            self._workload_bw += job_io_intensity
            self._system_bw += job_io_intensity

        num_pending_jobs = len(pending_jobs)
        num_running_jobs = len(running_jobs)
        self._workload_io_intensity = self._workload_bw / num_pending_jobs if num_pending_jobs else 0
        self._system_io_intensity = self._system_bw / num_running_jobs if num_running_jobs else 0

        return located_job

    def _get_best_configuration(self, job: dict[str, str], profiles: list[dict], num_available_nodes: int,
                                job_count: int) -> dict[str, int | str]:

        deltas = []
        job_id = job['jobid']
        size = int(job['size'])
        num_assigned_nodes = -(size // -self._procs_per_node)

        command = job['command']
        profiles = [profile for profile in profiles if profile['command'] == command]
        if len(profiles) < self._min_required_profiles:
            return {'result': 'retain_configuration', 'job_id': job_id}

        previous_configurations = [-(int(profile['size']) // -self._procs_per_node) for profile in profiles]
        min_size = min(previous_configurations) * self._procs_per_node
        max_size = max(previous_configurations) * self._procs_per_node

        query_id = profiles[0]['jobid']
        io_intensities = self._calculate_io_intensities(query_id, min_size, max_size + 1, self._procs_per_node)
        configurations = [configuration // self._procs_per_node for configuration in
                          range(min_size, max_size + 1, self._procs_per_node)]
        io_intensities = dict(zip(configurations, io_intensities))
        io_intensities = {configuration: io_intensity for configuration, io_intensity in io_intensities.items() if
                          configuration - num_assigned_nodes <= num_available_nodes}
        io_intensities.pop(num_assigned_nodes, None)

        if not io_intensities:
            return {'result': 'retain_configuration', 'job_id': job_id}

        job_io_intensity = self._calculate_io_intensity(query_id, size)
        for configuration, io_intensity in io_intensities.items():
            nodes_delta = configuration - num_assigned_nodes
            new_system_io_intensity = (self._system_bw - job_io_intensity + io_intensity) / job_count
            delta = abs(self._workload_io_intensity - new_system_io_intensity)
            deltas.append((delta, nodes_delta))

        deltas.sort(key=lambda x: x[0])
        delta, nodes_delta = deltas[0]

        if nodes_delta > 0 and delta / self._system_io_intensity > self._efficiency_threshold:
            return {'result': 'modify_configuration', 'job_id': job_id, 'delta': nodes_delta}
        else:
            return {'result': 'retain_configuration', 'job_id': job_id}

    def schedule(self, job_id: str, num_available_nodes: int) -> dict[str, str]:
        if not self._initialized:
            return {'result': 'error', 'message': 'I/O-intensity-aware scheduler has not been initialized'}
        if num_available_nodes < 1:
            return {'result': 'error', 'message': 'Number of available nodes can not be less than 1'}
        pending_jobs = self._metric_proxy.get_pending_jobs()
        running_jobs = self._metric_proxy.get_running_jobs()
        if not running_jobs:
            return {'result': 'error', 'message': 'List of running jobs is empty'}
        profiles = self._metric_proxy.get_profiles()
        located_job = self._update_io_intensities(pending_jobs, running_jobs, job_id, profiles)
        if located_job is None:
            return {'result': 'error', 'message': 'Job ID not found in running jobs', 'job_id': job_id}
        return self._get_best_configuration(located_job, profiles, num_available_nodes, len(running_jobs))
