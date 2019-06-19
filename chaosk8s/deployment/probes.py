from typing import Union

from chaoslib.exceptions import ActivityFailed
from chaoslib.types import Secrets
from kubernetes import client, watch
from logzero import logger

from chaosk8s import create_k8s_api_client

__all__ = ["deployment_available_and_healthy"]


def deployment_available_and_healthy(
        name: str, ns: str = "default",
        label_selector: str = "name in ({name})",
        secrets: Secrets = None) -> Union[bool, None]:
    """
    Lookup a deployment by `name` in the namespace `ns`.

    The selected resources are matched by the given `label_selector`.

    Raises :exc:`chaoslib.exceptions.ActivityFailed` when the state is not
    as expected.
    """
    label_selector = label_selector.format(name=name)
    api = create_k8s_api_client(secrets)

    v1 = client.AppsV1beta1Api(api)
    if label_selector:
        ret = v1.list_namespaced_deployment(ns, label_selector=label_selector)
    else:
        ret = v1.list_namespaced_deployment(ns)

    logger.debug("Found {d} deployment(s) named '{n}' in ns '{s}'".format(
        d=len(ret.items), n=name, s=ns))

    if not ret.items:
        raise ActivityFailed(
            "deployment '{name}' was not found".format(name=name))

    for d in ret.items:
        logger.debug("Deployment has '{s}' available replicas".format(
            s=d.status.available_replicas))

        if d.status.available_replicas != d.spec.replicas:
            raise ActivityFailed(
                "deployment '{name}' is not healthy".format(name=name))

    return True
