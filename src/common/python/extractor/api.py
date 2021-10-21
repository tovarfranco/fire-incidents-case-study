from requests.auth import HTTPBasicAuth
import requests
import pandas as pd

class SourceApi():
    def __init__(self,url,user,password):
        self.url = url,
        self.user = user,
        self.password = password
        
    def read(self):
        response = requests.get("https://data.sfgov.org/resource/wr8u-xric.json", auth=HTTPBasicAuth('qk17usuwcm5cwnns12ochy1k', '4fba91ry8usx3kgpb2n0uekj3kixl7lzz227khuzrfb9b79xlo')).json()
        df_response = pd.DataFrame(response)
        return df_response
