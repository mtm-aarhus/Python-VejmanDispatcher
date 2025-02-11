from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
import os

def sharepoint_client(username: str, password: str, sharepoint_site_url: str, orchestrator_connection: OrchestratorConnection) -> ClientContext:
    """
    Creates and returns a SharePoint client context.
    """
    # Authenticate to SharePoint
    ctx = ClientContext(sharepoint_site_url).with_credentials(UserCredential(username, password))

    # Load and verify connection
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()

    orchestrator_connection.log_info(f"Authenticated successfully. Site Title: {web.properties['Title']}")
    return ctx

    
def delete_sharepoint_folder(folder_path: str, ctx: ClientContext, orchestrator_connection: OrchestratorConnection):
    orchestrator_connection.log_info(f"Recursively deleting SharePoint folder: {folder_path}")

    target_folder = ctx.web.get_folder_by_server_relative_url(folder_path)
    ctx.load(target_folder)
    ctx.execute_query()

    # Delete all files in the folder
    files = target_folder.files
    ctx.load(files)
    ctx.execute_query()

    for file in files:
        orchestrator_connection.log_info(f"Deleting file: {file.serverRelativeUrl}")
        file.delete_object()
    ctx.execute_query()
    # Delete all subfolders recursively
    subfolders = target_folder.folders
    ctx.load(subfolders)
    ctx.execute_query()

    for subfolder in subfolders:
        delete_sharepoint_folder(subfolder.serverRelativeUrl, ctx, orchestrator_connection)

    # Now delete the folder itself
    target_folder.delete_object()
    ctx.execute_query()

    orchestrator_connection.log_info(f"Folder deleted: {folder_path}")
    
orchestrator_connection = OrchestratorConnection("VejmanDispatcher", os.getenv('OpenOrchestratorSQL'), os.getenv('OpenOrchestratorKey'), None)
RobotCredentials = orchestrator_connection.get_credential("Robot365User")
username = RobotCredentials.username
password = RobotCredentials.password

token = orchestrator_connection.get_credential("VejmanToken").password

SharePointTopFolder = "Delte dokumenter/TestTilladelser"

sharepoint_site_base = orchestrator_connection.get_constant("AarhusKommuneSharePoint").value
sharepoint_site = f"{sharepoint_site_base}/teams/tea-teamsite10014"

ctx = sharepoint_client(username, password, sharepoint_site, orchestrator_connection)
delete_sharepoint_folder(SharePointTopFolder, ctx, orchestrator_connection)
