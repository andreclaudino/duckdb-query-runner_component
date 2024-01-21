from kfp import dsl

def setup_resources(operator: dsl.ContainerOp, memory_request: str, memory_limit: str,
                    cpu_request: str, cpu_limit: str) -> dsl.ContainerOp:
    operator.container \
        .set_memory_request(memory_request) \
        .set_memory_limit(memory_limit) \
        .set_cpu_request(cpu_request) \
        .set_cpu_limit(cpu_limit)

    return operator