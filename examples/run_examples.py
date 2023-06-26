import itertools
from glob import glob
from subprocess import PIPE, Popen

maven_openlineage = [
    "io.openlineage:openlineage-spark:0.20.4",
    "io.openlineage:openlineage-spark:0.20.6",
    "io.openlineage:openlineage-spark:0.21.1",
    "io.openlineage:openlineage-spark:0.22.0",
    "io.openlineage:openlineage-spark:0.23.0",
    "io.openlineage:openlineage-spark:0.24.0",
    "io.openlineage:openlineage-spark:0.25.0",
    "io.openlineage:openlineage-spark:0.26.0",
]

maven_openlineage = [maven_openlineage[-1]]

list_pipelines = glob('examples/pipelines/*.py')

pipes_return_code = []

for python_file, openlineagejar in itertools.product(list_pipelines, maven_openlineage):
    command = ['python', python_file, '--openlineagejar', openlineagejar]

    p = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    exit_code = p.wait()

    list_pipelines.append(
        {"arquivo": python_file, 'openlineagejar': openlineagejar, "result": exit_code}
    )

    if exit_code != 0:
        print(python_file, openlineagejar)
        print(stdout)
    else:
        print(python_file, openlineagejar)
