from kfp import dsl
from kubernetes.client import V1EnvVar, V1SecretKeySelector, V1EnvVarSource

from athena_query_runner.constants import AWS_ACCESS_KEY_ID_ENV, AWS_REGION_ENV, AWS_SECRET_ACCESS_KEY_ENV


def setup_environment_variables(operator: dsl.ContainerOp, aws_secret_name: str) -> dsl.ContainerOp:
    operator = _add_env_from_secret(operator, aws_secret_name, AWS_ACCESS_KEY_ID_ENV, AWS_ACCESS_KEY_ID_ENV)
    operator = _add_env_from_secret(operator, aws_secret_name, AWS_SECRET_ACCESS_KEY_ENV, AWS_SECRET_ACCESS_KEY_ENV)
    operator = _add_env_from_secret(operator, aws_secret_name, AWS_REGION_ENV, AWS_REGION_ENV)
    
    return operator


def _add_env_from_secret(operator: dsl.ContainerOp, secret_name: str, env_name: str, secret_key: str) -> dsl.ContainerOp:
    operator.container.add_env_variable(V1EnvVar(
        name=env_name,
        value_from=V1EnvVarSource(
            secret_key_ref=V1SecretKeySelector(
                name=secret_name,
                key=secret_key
            )
        )
    ))
    return operator