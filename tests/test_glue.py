import os
from glue.glue_pipe import AdaquantGlueCatalogOrchestrator, GlueConfig


if __name__ == "__main__":
    config = GlueConfig(
        region="eu-south-2",
        group="imat3a02",
        bucket_name="adaquant-data-bucket",
        glue_role_arn=os.environ["GLUE_ROLE_ARN"],
    )

    glue = AdaquantGlueCatalogOrchestrator(config)

    crawler_name = glue.ensure_and_run(
        crypto="ada",
        table_prefix="ada_",
        wait=True,
        verbose=True,
    )

print("Crawler used:", crawler_name)
