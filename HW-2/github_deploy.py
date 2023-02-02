from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/Manojkl/data-engineering-manojkl",

)
block.get_directory("HW-2") # specify a subfolder of repo
block.save("github-zoomcamp-test")

# from prefect.deployments import Deployment
# from etl_web_to_gcs import etl_web_to_gcs
# from prefect.infrastructure.docker import DockerContainer
# from prefect.filesystems import GitHub

# # github_block = DockerContainer.load("github-zoomcamp")
# github_block = GitHub.load("github-zoomcamp")

# github_dep = Deployment.build_from_flow(
#     flow=etl_web_to_gcs,
#     name="github-flow",
#     infrastructure=github_block,
# )

# if __name__ == "__main__":
#     github_dep.apply()

# from prefect.deployments import Deployment
# from parameterized_flow import etl_parent_flow
# from prefect.infrastructure.docker import DockerContainer

# docker_block = DockerContainer.load("zoom")

# docker_dep = Deployment.build_from_flow(
#     flow=etl_parent_flow,
#     name="docker-flow-new",
#     infrastructure=docker_block,
# )

# if __name__ == "__main__":
#     docker_dep.apply()
