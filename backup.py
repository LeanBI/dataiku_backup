from dataikuapi.dssclient import DSSClient
from os import environ,path
import sys
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError, AuthError
    
    
class dataiku_server(DSSClient):
    def __init__(self,host,api_key):
        DSSClient.__init__(self, host, api_key)
        self.backends={}
        
    def export_all(self,target_dir=None):
        export_options={
                      "exportUploads" : True
                    , "exportManagedFS": True
                    , "exportAnalysisModels": True
                    , "exportSavedModels": True
                    , "exportManagedFolders": False
                    , "exportAllInputDatasets" : True
                    , "exportAllDatasets" : True
                    , "exportAllInputManagedFolders" : False
                    , "exportGitRepository" : False
                    }

        exported=[]
        dss_projects = self.list_project_keys()
        for p in dss_projects:
            target=target_dir + p + ".zip"
            print("exporting project=%s to %s" % (p,target))
            project=self.get_project(p)
            project.export_to_file(target,options=export_options)#options={'whatever':True}
            print("done")
            exported.append(target)
        print ("all project exported")
        return exported
    
    def backup_to_drobox(self,dl_dir,target_dir,token):
        if "dropbox" in self.backends:
            dbx=self.backends
        else :
            dbx=dropbox_backend(token)
            self.backends["dropbox"]=dbx
        
        backup_files=self.export_all(dl_dir)
        for b in backup_files:
            dbx.backup(b, target_dir)
            
            
class dropbox_backend(dropbox.Dropbox):
    def __init__(self,token=None):
        if token==None :
            token=environ["DROPBOX_TOKEN"]
            
        if (len(token) == 0):
            sys.exit("ERROR: Looks like you didn't add your access token")
        
        dropbox.Dropbox.__init__(self,token,timeout=None)
        try:
            self.users_get_current_account()
        except AuthError as err:
            sys.exit("ERROR: Invalid access token; try re-generating an access token from the app console on the web.")

    def backup(self,LOCALFILE,BACKUPPATH):
        with open(LOCALFILE, 'rb') as f:
            # We use WriteMode=overwrite to make sure that the settings in the file
            # are changed on upload
            BACKUPPATH=BACKUPPATH + path.basename(LOCALFILE)
            print("Uploading " + LOCALFILE + " to Dropbox as " + BACKUPPATH  + "...")
            try:
                self.files_upload(f, BACKUPPATH , mode=WriteMode('overwrite'))
            except ApiError as err:
                # This checks for the specific error where a user doesn't have
                # enough Dropbox space quota to upload this file
                if (err.error.is_path() and
                        err.error.get_path().error.is_insufficient_space()):
                    sys.exit("ERROR: Cannot back up; insufficient space.")
                elif err.user_message_text:
                    print(err.user_message_text)
                    sys.exit()
                else:
                    print(err)
                    sys.exit()
                    
                            
if __name__ == "__main__" :
    
    dl_dir= environ["BACKUP_DOWNLOAD_DIR"] or "/tmp"
    host = environ["DATAIKU_HOST"] # http://localhost:10000
    hostname = environ["DATAIKU_HOSTNAME"]
    apiKey = environ["DATAIKU_APIKEY"]
    dpx_token= environ.get("DPX_TOKEN",None)
    target_dir= environ["BACKUP_DIR"] +  hostname + "/"
    server=dataiku_server(host,apiKey)
    if dpx_token != None :
        server.backup_to_drobox(dl_dir, target_dir, dpx_token)
    else :
        server.export_all(dl_dir)
        
    