import redis
import argparse

from malleability_manager import MetricProxyAdapter, IOIntensityAwareScheduler


class Client:

    def __init__(self, redis_host: str, redis_port: int, metric_proxy_host: str, metric_proxy_port: int) -> None:
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.metric_proxy = MetricProxyAdapter('http://' + metric_proxy_host + ':' + str(metric_proxy_port))

    def run(self) -> None:
        entry = self.redis.xread({'malleability_manager': '$'}, block=0)[0][1][0]
        last_seen_key, message = entry
        scheduler = IOIntensityAwareScheduler()
        result = scheduler.initialize_algorithm(message, metric_proxy=self.metric_proxy)
        if result is not None:
            self.redis.xadd('intelligent_controller', result)
            return
        while True:
            entry = self.redis.xread({'malleability_manager': last_seen_key}, block=0)[0][1][0]
            last_seen_key, message = entry
            if 'command' not in message:
                result = {'result': 'error', 'message': '"command" expected in message but is missing'}
                self.redis.xadd('intelligent_controller', result)
                continue
            if message['command'] == 'invoke':
                if 'job_id' not in message:
                    result = {'result': 'error', 'message': '"job_id" expected in message but is missing'}
                    self.redis.xadd('intelligent_controller', result)
                    continue
                if 'num_available_nodes' not in message:
                    result = {'result': 'error', 'message': '"num_available_nodes" expected in message but is missing'}
                    self.redis.xadd('intelligent_controller', result)
                    continue
                num_available_nodes = int(message['num_available_nodes'])
                result = scheduler.schedule(message['job_id'], num_available_nodes)
                self.redis.xadd('intelligent_controller', result)
            elif message['command'] == 'finalize':
                break
            else:
                result = {'result': 'error', 'message': f'Unknown command {message["command"]}'}
                self.redis.xadd('intelligent_controller', result)
                break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--redis_host', default='localhost', help='Redis host')
    parser.add_argument('--redis_port', type=int, default=6379, help='Redis port number')
    parser.add_argument('--metric_proxy_host', default='localhost', help='Metric proxy host')
    parser.add_argument('--metric_proxy_port', type=int, default=1337, help='Metric proxy port number')
    args = parser.parse_args()
    malleability_manager = Client(args.redis_host, args.redis_port, args.metric_proxy_host,
                                  args.metric_proxy_port)
    malleability_manager.run()
