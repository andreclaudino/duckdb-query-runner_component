from kfp import dsl


from athena_query_runner.constants import DEFAULT_AWS_SECRET_NAME, DEFAULT_DATABASE, DEFAULT_IMAGE_NAME, DEFAULT_IMAGE_TAG, DEFAULT_LOG_LEVEL,\
    DEFAULT_QUERY_COMPLETION_POOLING_TIME, DEFAULT_S3_OUTPUT_LOCATION, DEFAULT_MEMORY_LIMIT, DEFAULT_CPU_LIMIT, DEFAULT_CPU_REQUEST, \
    DEFAULT_MEMORY_REQUEST, STARTUP_COMMAND, DEFAULT_DISPLAY_NAME, DEFAULT_CONTAINER_NAME, OUTPUT_PATH
from athena_query_runner.resources import setup_resources
from athena_query_runner.secrets import setup_environment_variables



def make_athena_query_runner_component(query_file_path: str, database: str = DEFAULT_DATABASE, s3_output_location: str = DEFAULT_S3_OUTPUT_LOCATION,
        query_completion_pooling_time: int = DEFAULT_QUERY_COMPLETION_POOLING_TIME, log_level: str = DEFAULT_LOG_LEVEL, display_name: str = DEFAULT_DISPLAY_NAME,
        image_name: str = DEFAULT_IMAGE_NAME, image_tag: str = DEFAULT_IMAGE_TAG, aws_secret_name: str = DEFAULT_AWS_SECRET_NAME,
        memory_request: str = DEFAULT_MEMORY_REQUEST, memory_limit: str = DEFAULT_MEMORY_LIMIT, cpu_request: str = DEFAULT_CPU_REQUEST, cpu_limit: str = DEFAULT_CPU_LIMIT
) -> dsl.ContainerOp:
    image = f"{image_name}:{image_tag}"

    operator = dsl.ContainerOp(
        name=DEFAULT_CONTAINER_NAME,
        image=image,
        command=STARTUP_COMMAND,
        arguments=[
            "--s3-output-location", s3_output_location,
            "--athena-database", database,
            "--query-file-path", query_file_path,
            "--query-completion-pooling-time", query_completion_pooling_time,
            "--output-path", OUTPUT_PATH,
            "--log-level", log_level
        ]
    )
    operator.set_display_name(display_name)
    operator = setup_resources(operator, memory_request, memory_limit, cpu_request, cpu_limit)
    operator = setup_environment_variables(operator, aws_secret_name)

    return operator
