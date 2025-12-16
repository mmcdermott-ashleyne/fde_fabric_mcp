# from __future__ import annotations
# import os
# from typing import Dict, Any

# # Insert your entire FabricPipelineRunner definition here unchanged.
# # (Not repeating due to size — but copy your original class directly.)

# from .pipeline_runner_full_impl import FabricPipelineRunner, DEFAULT_ROUTES  # optional split


# def build_runner_from_env() -> FabricPipelineRunner:
#     return FabricPipelineRunner(
#         tenant_id=os.environ["FABRIC_TENANT_ID"],
#         client_id=os.environ["FABRIC_CLIENT_ID"],
#         client_secret=os.environ["FABRIC_CLIENT_SECRET"],
#         default_workspace_id=os.environ["FABRIC_DEFAULT_WORKSPACE_ID"],
#         routes=DEFAULT_ROUTES,
#         sql_user=os.environ["FABRIC_SQL_USER"],
#         sql_password=os.environ["FABRIC_SQL_PASSWORD"],
#         sql_driver=os.getenv("FABRIC_SQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
#         config_database_name=os.getenv("FABRIC_CONFIG_DB", "core_dw_config"),
#         project_database_name=os.getenv("FABRIC_PROJECT_DB", "core_dw"),
#         sql_profile_for_config=os.getenv("FABRIC_SQL_PROFILE_CONFIG", "config"),
#     )
