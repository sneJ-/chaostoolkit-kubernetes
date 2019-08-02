# -*- coding: utf-8 -*-
import math
import random
import re

from chaoslib.exceptions import ActivityFailed
from chaoslib.types import Secrets
from kubernetes import client
from logzero import logger

from chaosk8s import create_k8s_api_client

__all__ = ["terminate_pods", "kill_main_process"]


def kill_main_process(label_selector: str = None, name_pattern:
                      str = None, all: bool = False, rand:
                      bool = False, mode: str = "fixed", qty: int = 1,
                      ns: str = "default", order: str = "alphabetic",
                      container: str = "*", signal: str = "SIGTERM",
                      pumba_image: str = "gaiaadm/pumba:master",
                      secrets: Secrets = None):
    """
    Kill the main process in a pod's container. Select the appropriate pods
    by label and/or name patterns. Whenever a pattern is provided for the
    name, all pods retrieved will be filtered out if their name do not match
    the given pattern.

    If neither `label_selector` nor `name_pattern` are provided, all pods
    in the namespace will be selected to kill their container's main process.

    If `all` is set to `True`, all matching pods will terminate ther main
    containers processes.

    If `container` is set to `*` all main processes in all containers of the
    pod will be killed. Otherwise, only the containers that match the given
    name in a pod.

    The parameter `signal` defines which kill signal is sent to the pod's main
    container. By default a `SIGTERM` signal is sent.

    The parameter `ns` defines the namespace in which pods are being selected.
    By default the `default` namespace is used.

    Value of `qty` varies based on `mode`.
    If `mode` is set to `fixed`, then `qty` refers to number of pods that kill
    their container's main processes. If `mode` is set to `percentage`, then
    `qty` refers to percentage of pods, from 1 to 100, that kill their
    container's main processes. Default `mode` is `fixed` and default `qty`
    is `1`.

    If `order` is set to `oldest`, the retrieved pods will be ordered
    by the pods creation_timestamp, with the oldest pod first in list.

    If `rand` is set to `True`, n random pods will be chosen to kill their
    container's main processes. Otherwise, the first retrieved n pods
    be chosen to kill their container's main process.

    With the parameter `pumba_image` one can change the image of pumba to
    delete processes. It defaults to the current master of the project.
    """

    # determine the pods to kill
    pods_to_kill = _select_pods(label_selector=label_selector,
                                name_pattern=name_pattern,
                                all=all, rand=rand,
                                mode=mode, qty=qty,
                                ns=ns, order=order,
                                secrets=secrets)

    # initiate pumba pods on the nodes which accommodate the pods whose
    # processes to kill
    api = create_k8s_api_client(secrets)

    v1 = client.CoreV1Api(api)

    i = 0
    for pod in pods_to_kill:
        pumba_pod = client.V1Pod()
        pumba_pod.metadata = client.V1ObjectMeta(name="pumba-pod-%d" % (i,))
        pumba_pod.metadata.labels = {
            "app": "pumba",
            "com.gaiaadm.pumba": "true",
            "container": container,
            "pod": pod.metadata.name,
            "namespace": ns
        }
        pumba_pod.node_name = pod.spec.node_name
        container = client.V1Container(name="pumba")
        container.image = pumba_image
        container.image_pull_policy = "Always"
        container.args = ["--log-level", "debug", "kill", "--signal", signal,
                          "re2:^k8s_%s_%s_%s" % (container, pod.metadata.name,
                                                 ns)]
        resources = client.V1ResourceRequirements(
            requests={
                    "cpu": "10m",
                    "memory": "5M"
                },
            limits={
                    "cpu": "100m",
                    "memory": "20M"
                })
        container.resources = resources
        volumeMount = client.V1VolumeMount(mount_path="/var/run/docker.sock",
                                           name="dockersocket")
        container.volumeMounts = [volumeMount]
        spec = client.V1PodSpec(containers=[container])
        spec.restart_policy = "Never"
        host_path = client.V1HostPathVolumeSource(path="/var/run/docker.sock")
        volume = client.V1Volume(name="dockersocket", host_path=host_path)
        spec.volumes = [volume]
        pumba_pod.spec = spec
        v1.create_namespaced_pod(ns, pumba_pod)
        i += 1


def terminate_pods(label_selector: str = None, name_pattern: str = None,
                   all: bool = False, rand: bool = False,
                   mode: str = "fixed", qty: int = 1,
                   grace_period: int = -1,
                   ns: str = "default", order: str = "alphabetic",
                   secrets: Secrets = None):
    """
    Terminate a pod gracefully. Select the appropriate pods by label and/or
    name patterns. Whenever a pattern is provided for the name, all pods
    retrieved will be filtered out if their name do not match the given
    pattern.

    If neither `label_selector` nor `name_pattern` are provided, all pods
    in the namespace will be selected for termination.

    If `all` is set to `True`, all matching pods will be terminated.

    The parameter `ns` defines the namespace in which pods are being selected.
    By default the `default` namespace is used.

    Value of `qty` varies based on `mode`.
    If `mode` is set to `fixed`, then `qty` refers to number of pods to be
    terminated. If `mode` is set to `percentage`, then `qty` refers to
    percentage of pods, from 1 to 100, to be terminated.
    Default `mode` is `fixed` and default `qty` is `1`.

    If `order` is set to `oldest`, the retrieved pods will be ordered
    by the pods creation_timestamp, with the oldest pod first in list.

    If `rand` is set to `True`, n random pods will be terminated.
    Otherwise, the first retrieved n pods will be terminated.

    If `grace_period` is greater than or equal to 0, it will
    be used as the grace period (in seconds) to terminate the pods.
    Otherwise, the default pod's grace period will be used.
    """

    pods = _select_pods(label_selector=label_selector,
                        name_pattern=name_pattern,
                        all=all, rand=rand,
                        mode=mode, qty=qty,
                        ns=ns, order=order,
                        secrets=secrets)

    logger.debug("Picked pods '{p}' to be terminated".format(
        p=",".join([po.metadata.name for po in pods])))

    api = create_k8s_api_client(secrets)

    v1 = client.CoreV1Api(api)

    body = client.V1DeleteOptions()
    if grace_period >= 0:
        body = client.V1DeleteOptions(grace_period_seconds=grace_period)

    for p in pods:
        res = v1.delete_namespaced_pod(p.metadata.name, ns, body=body)


def _select_pods(label_selector: str = None, name_pattern: str = None,
                 all: bool = False, rand: bool = False,
                 mode: str = "fixed", qty: int = 1,
                 ns: str = "default", order: str = "alphabetic",
                 secrets: Secrets = None):
    """
    Select the appropriate pods by label and/or
    name patterns. Whenever a pattern is provided for the name, all pods
    retrieved will be filtered out if their name do not match the given
    pattern.

    If neither `label_selector` nor `name_pattern` are provided, all pods
    in the namespace will be selected.

    If `all` is set to `True`, all matching pods will be selected.

    The parameter `ns` defines the namespace in which pods are being selected.
    By default the `default` namespace is used.

    Value of `qty` varies based on `mode`.
    If `mode` is set to `fixed`, then `qty` refers to number of pods to be
    selected. If `mode` is set to `percentage`, then `qty` refers to
    percentage of pods, from 1 to 100, to be selected.
    Default `mode` is `fixed` and default `qty` is `1`.

    If `order` is set to `oldest`, the retrieved pods will be ordered
    by the pods creation_timestamp, with the oldest pod first in list.

    If `rand` is set to `True`, n random pods will be selected.
    Otherwise, the first retrieved n pods will be selected.
    """
# Fail when quantity is less than 0
    if qty < 0:
        raise ActivityFailed(
            "Cannot terminate pods. Quantity '{q}' is negative.".format(q=qty))
    # Fail when mode is not `fixed` or `percentage`
    if mode not in ['fixed', 'percentage']:
        raise ActivityFailed(
            "Cannot terminate pods. Mode '{m}' is invalid.".format(m=mode))
    # Fail when order not `alphabetic` or `oldest`
    if order not in ['alphabetic', 'oldest']:
        raise ActivityFailed(
            "Cannot terminate pods. Order '{o}' is invalid.".format(o=order))
    api = create_k8s_api_client(secrets)

    v1 = client.CoreV1Api(api)
    if label_selector:
        ret = v1.list_namespaced_pod(ns, label_selector=label_selector)
        logger.debug("Found {d} pods labelled '{s}' in ns {n}".format(
            d=len(ret.items), s=label_selector, n=ns))
    else:
        ret = v1.list_namespaced_pod(ns)
        logger.debug("Found {d} pods in ns '{n}'".format(
            d=len(ret.items), n=ns))

    pods = []
    if name_pattern:
        pattern = re.compile(name_pattern)
        for p in ret.items:
            if pattern.match(p.metadata.name):
                pods.append(p)
                logger.debug("Pod '{p}' match pattern".format(
                    p=p.metadata.name))
    else:
        pods = ret.items

    if order == 'oldest':
        pods.sort(key=_sort_by_pod_creation_timestamp)
    if not all:
        if mode == 'percentage':
            qty = math.ceil((qty * len(pods)) / 100)
        # If quantity is greater than number of pods present, cap the
        # quantity to maximum number of pods
        qty = min(qty, len(pods))

        if rand:
            pods = random.sample(pods, qty)
        else:
            pods = pods[:qty]
    return pods


def _sort_by_pod_creation_timestamp(pod):
    return pod.metadata.creation_timestamp
