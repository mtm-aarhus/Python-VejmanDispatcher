from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
import pyodbc
import requests
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

import time
from datetime import datetime, timedelta
import requests
import os
import json
import re

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

def create_sharepoint_folder(folder_name, ctx: ClientContext, orchestrator_connection: OrchestratorConnection):
    print(f"Creating SharePoint folder: {folder_name}")
    toplevel_folder = "/".join(folder_name.split("/")[:-1])
    new_folder = folder_name.split("/")[-1]
    target_folder = ctx.web.get_folder_by_server_relative_url(toplevel_folder)
    ctx.load(target_folder)
    ctx.execute_query()
    target_folder.folders.add(new_folder).execute_query()

def rename_sharepoint_folder(old_name, new_name, ctx: ClientContext, orchestrator_connection: OrchestratorConnection):
    print(f"Renaming SharePoint folder: {old_name} → {new_name}")
    target_folder = ctx.web.get_folder_by_server_relative_url(old_name)
    ctx.load(target_folder)
    ctx.execute_query()
    target_folder.rename(new_name).execute_query()

    
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
    target_folder.delete_object()
    ctx.execute_query()

    print(f"Folder deleted: {folder_path}")

def sanitize_folder_name(folder_name):
    pattern = r'[~#%&*{}\[\]\\:<>?/+|$¤£€\"\t]'
    folder_name = re.sub(pattern, "", folder_name)
    folder_name = re.sub(r"\s+", " ", folder_name).strip()
    return folder_name
    

orchestrator_connection = OrchestratorConnection("VejmanDispatcher", os.getenv('OpenOrchestratorSQL'), os.getenv('OpenOrchestratorKey'), None)
RobotCredentials = orchestrator_connection.get_credential("Robot365User")
username = RobotCredentials.username
password = RobotCredentials.password

token = orchestrator_connection.get_credential("VejmanToken").password

SharePointTopFolder = "Delte dokumenter/TestTilladelser"

today = datetime.today().strftime('%Y-%m-%d')
future_date = (datetime.today()+timedelta(days=4)).strftime('%Y-%m-%d')

#Get GraveMaterielTilladelser
URL = f"https://vejman.vd.dk/permissions/getcases?pmCaseStates=3&pmCaseFields=state%2Ctype%2Ccase_number%2Cstreet_name%2Cinitials&pmCaseWorker=all&pmCaseTypes=%27rovm%27&pmCaseVariant=all&pmCaseTags=ignorerTags&pmCaseShowAttachments=false&dontincludemap=1&startDateTo={future_date}&endDateFrom={today}&_={int(time.time()*1000)}&token={token}"
# Fetch JSON data
response = requests.get(URL)
json_data = response.json()

# SQL Server connection
sql_server = orchestrator_connection.get_constant("SqlServer")
conn_string = f"DRIVER={{SQL Server}};SERVER={sql_server.value};DATABASE=PYORCHESTRATOR;Trusted_Connection=yes;"
conn = pyodbc.connect(conn_string)
cursor = conn.cursor()

sharepoint_site_base = orchestrator_connection.get_constant("AarhusKommuneSharePoint").value
sharepoint_site = f"{sharepoint_site_base}/teams/tea-teamsite10014"

ctx = sharepoint_client(username, password, sharepoint_site, orchestrator_connection)
# Extract cases
cases = json_data.get("cases", [])


#Iterate through each case
for case in cases:
    case_id = case["case_id"]
    case_number = case["case_number"]
    vejnavn = case["street_name"]

    # Generate expected SharePoint folder name (removing illegal characters)
    if not vejnavn:
        vejnavn = "Intet vejnavn angivet"
    expected_folder_name = sanitize_folder_name(vejnavn.replace(".", "") + "_" + case_number)
    sharepoint_folder_path = SharePointTopFolder+"/"+expected_folder_name
    # Check if the row exists in the database and is not deleted
    cursor.execute("""
        SELECT SharePointFolder FROM [dbo].[VejmanTilladelser] 
        WHERE ID = ?
    """, (case_id,))
    
    row = cursor.fetchone()

    if row:
        existing_folder = row[0]
        if existing_folder != sharepoint_folder_path:
            print(f"Renaming SharePoint folder for case {case_number}")

            # Rename the folder in SharePoint
            rename_sharepoint_folder(existing_folder, expected_folder_name, ctx, orchestrator_connection)

            # Update database with the new folder name
            cursor.execute("""
                UPDATE [dbo].[VejmanTilladelser] 
                SET SharePointFolder = ?, LastUpdated = GETDATE() 
                WHERE ID = ?
            """, (sharepoint_folder_path, case_id))
            conn.commit()
        
        else:
            # print(f"Folder for case {case_number} is already correct. Updating LastUpdated.")

            cursor.execute("""
                UPDATE [dbo].[VejmanTilladelser] 
                SET LastUpdated = GETDATE() 
                WHERE ID = ?
            """, (case_id,))
            conn.commit()

    else:  # Case does not exist in the database
        print(f"Creating new entry for case {case_number}")

        # Create SharePoint folder
        create_sharepoint_folder(sharepoint_folder_path, ctx, orchestrator_connection)

        # Insert into the database
        cursor.execute("""
            INSERT INTO [dbo].[VejmanTilladelser] (ID, CaseNumber, SharePointFolder, LastUpdated) 
            VALUES (?, ?, ?, GETDATE())
        """, (case_id, case_number, sharepoint_folder_path))
        conn.commit()

    # Prepare JSON payload for queue element
    payload = json.dumps({
        "case_id": case_id,
        "case_number": case_number,
        "sharepoint_folder": sharepoint_folder_path,

    }, ensure_ascii=False)  # Preserve special characters

    # Create queue element
    orchestrator_connection.create_queue_element("VejmanPerformer", reference=case_number, data=payload)

print(f"Processed {len(cases)} cases.")

### DELETE SHAREPOINT FOLDERS NOT UPDATED IN 30 DAYS ###
thirty_days_ago = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

# Fetch folders that are older than 30 days and not marked as deleted
cursor.execute("""
    SELECT ID, SharePointFolder FROM [dbo].[VejmanTilladelser]
    WHERE LastUpdated < ?
""", (thirty_days_ago,))

old_folders = cursor.fetchall()

for folder in old_folders:
    case_id, folder_path = folder

    print(f"Deleting old SharePoint folder: {folder_path}")

    # Delete folder in SharePoint (recursive)
    delete_sharepoint_folder(folder_path, ctx, orchestrator_connection)

    # Delete row from SQL Server
    cursor.execute("""
        DELETE FROM [dbo].[VejmanTilladelser] 
        WHERE ID = ?
    """, (case_id,))
    conn.commit()

print(f"Deleted {len(old_folders)} old folders from SharePoint.")

# Close SQL connection
cursor.close()
conn.close()