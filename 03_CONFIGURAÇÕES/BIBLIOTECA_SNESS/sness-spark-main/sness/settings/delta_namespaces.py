from sness.settings import NAMESPACES_TO_CONVERT


def convert_namespace_to_delta(namespace):
    if len(NAMESPACES_TO_CONVERT) > 0:
        namespaces_allowed = NAMESPACES_TO_CONVERT
        if namespace in namespaces_allowed:
            return True
        return False
