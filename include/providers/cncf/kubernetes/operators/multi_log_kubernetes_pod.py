import math
import time
from functools import cached_property
from typing import Optional, Iterable

import pendulum
import tenacity
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodLoggingStatus
from kubernetes.client.models.v1_pod import V1Pod
from pendulum import DateTime
from urllib3.exceptions import HTTPError as BaseHTTPError

from include.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


class MultiLogPodManagerVerbose(PodManager):
    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    def read_pod_logs(
        self,
        pod: V1Pod,
        container_name: str,
        tail_lines: Optional[int] = None,
        timestamps: bool = False,
        since_seconds: Optional[int] = None,
        follow=True,
    ) -> Iterable[bytes]:
        """Reads log from the POD"""
        additional_kwargs = {}
        if since_seconds:
            additional_kwargs['since_seconds'] = since_seconds

        if tail_lines:
            additional_kwargs['tail_lines'] = tail_lines

        try:
            return self._client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container=container_name,
                follow=follow,
                timestamps=timestamps,
                _preload_content=False,
                **additional_kwargs,
            )
        except BaseHTTPError:
            self.log.exception('There was an error reading the kubernetes API.')
            raise

    def fetch_container_logs(
        self, pod: V1Pod, container_name: str, *, follow=False, since_time: Optional[DateTime] = None
    ) -> PodLoggingStatus:
        """
        Follows the logs of container and streams to airflow logging.
        Returns when container exits.
        """

        def consume_logs(*, _container_name: str, since_time: Optional[DateTime] = None, follow: bool = True) -> Optional[DateTime]:
            """
            Tries to follow container logs until container completes.
            For a long-running container, sometimes the log read may be interrupted
            Such errors of this kind are suppressed.

            Returns the last timestamp observed in logs.
            """
            timestamp = None
            try:
                logs = self.read_pod_logs(
                    pod=pod,
                    container_name=_container_name,
                    timestamps=True,
                    since_seconds=(
                        math.ceil((pendulum.now() - since_time).total_seconds()) if since_time else None
                    ),
                    follow=follow,
                )
                for raw_line in logs:
                    line = raw_line.decode('utf-8', errors="backslashreplace")
                    timestamp, message = self.parse_log_line(line)
                    self.log.info(message)
            except BaseHTTPError as e:
                self.log.warning(
                    "Reading of logs interrupted with error %r; will retry. "
                    "Set log level to DEBUG for traceback.",
                    e,
                )
                self.log.debug(
                    "Traceback for interrupted logs read for pod %r",
                    pod.metadata.name,
                    exc_info=True,
                )
            return timestamp or since_time

        # note: `read_pod_logs` follows the logs, so we shouldn't necessarily *need* to
        # loop as we do here. But in a long-running process we might temporarily lose connectivity.
        # So the looping logic is there to let us resume following the logs.
        last_log_time = since_time
        while True:
            last_log_time = consume_logs(_container_name=container_name, since_time=last_log_time, follow=follow)
            self.log.info(f"!!! CONSUMED LOGS !!!")
            if not self.container_is_running(pod, container_name=container_name):
                self.log.info(f"!!! MAIN POD FINISHED !!!")
                # After the main pod is done - log all the sidecars
                all_other_containers = [c.name for c in pod.spec.containers if c.name != container_name]
                self.log.info(f"!!! GOT {len(all_other_containers)} CONTAINERS MORE !!!")
                for other_container_name in all_other_containers:
                    self.log.info(f"!!! CONSUMING LOGS OF {other_container_name} !!!")
                    consume_logs(_container_name=other_container_name, follow=False)

                self.log.info(f"!!! RETURNING !!!")
                return PodLoggingStatus(running=False, last_log_time=last_log_time)

            # Follow is always true
            if not follow:
                return PodLoggingStatus(running=True, last_log_time=last_log_time)
            else:
                self.log.warning(
                    'Pod %s log read interrupted but container %s still running',
                    pod.metadata.name,
                    container_name,
                )
                time.sleep(1)


class MultiLogPodManager2(PodManager):
    def fetch_container_logs(
        self, pod: V1Pod, container_name: str, *, follow=False, since_time: Optional[DateTime] = None
    ) -> PodLoggingStatus:
        """
        Follows the logs of container and streams to airflow logging.
        Returns when container exits.
        """
        last_log_time = since_time
        try:
            logs = self._client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container=container_name,
                follow=follow,
                _preload_content=False,
                # **additional_kwargs,
            )
        except BaseHTTPError:
            self.log.exception('There was an error reading the kubernetes API.')
            raise
        for raw_line in logs:
            line = raw_line.decode('utf-8', errors="backslashreplace")
            timestamp, message = self.parse_log_line(line)
            self.log.info(message)
        return PodLoggingStatus(running=False, last_log_time=last_log_time)
        # return PodLoggingStatus(running=True, last_log_time=last_log_time)


class MultiLogKubernetesPodOperator(KubernetesPodOperator):
    @cached_property
    def pod_manager(self) -> PodManager:
        return MultiLogPodManager(kube_client=self.client)
