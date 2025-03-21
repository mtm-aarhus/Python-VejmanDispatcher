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

    print(f"Authenticated successfully. Site Title: {web.properties['Title']}")
    return ctx

    
def delete_sharepoint_folder(folder_path: str, ctx: ClientContext, orchestrator_connection: OrchestratorConnection):
    print(f"Recursively deleting SharePoint folder: {folder_path}")

    target_folder = ctx.web.get_folder_by_server_relative_url(folder_path)
    ctx.load(target_folder)
    ctx.execute_query()

    # Delete all files in the folder
    files = target_folder.files
    ctx.load(files)
    ctx.execute_query()

    for file in files:
        print(f"Deleting file: {file.serverRelativeUrl}")
        file.delete_object()
    ctx.execute_query()
    # Delete all subfolders recursively
    subfolders = target_folder.folders
    ctx.load(subfolders)
    ctx.execute_query()

    for subfolder in subfolders:
        delete_sharepoint_folder(subfolder.serverRelativeUrl, ctx, orchestrator_connection)

    # Now delete the folder itself
    if not folder_path == 'Delte dokumenter/Tilladelser':
        target_folder.delete_object()
    ctx.execute_query()

    print(f"Folder deleted: {folder_path}")

  
orchestrator_connection = OrchestratorConnection("VejmanDispatcher", os.getenv('OpenOrchestratorSQL'), os.getenv('OpenOrchestratorKey'), None)
RobotCredentials = orchestrator_connection.get_credential("Robot365User")
username = RobotCredentials.username
password = RobotCredentials.password

SharePointTopFolder = "Delte dokumenter/Gamle mapper"

sharepoint_site_base = orchestrator_connection.get_constant("AarhusKommuneSharePoint").value
sharepoint_site = f"{sharepoint_site_base}/teams/tea-teamsite10014"

MAX_RETRIES = 10  # Set the max retry attempts

for attempt in range(1, MAX_RETRIES + 1):
    try:
        # Create a new SharePoint context
        ctx = sharepoint_client(username, password, sharepoint_site, orchestrator_connection)
        
        # Attempt to delete the folder
        delete_sharepoint_folder(SharePointTopFolder, ctx, orchestrator_connection)
        
        # If successful, break out of the loop
        print("Folder deletion successful")
        break
    except Exception as e:
        print(f"Attempt {attempt} failed: {e}")

        if attempt < MAX_RETRIES:
            print("Retrying with a new context...")
        else:
            print("Max retries reached. Could not delete folder.")
            raise  # Reraise the last exception if all retries fail
