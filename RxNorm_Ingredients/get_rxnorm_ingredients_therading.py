import pandas as pd
import concurrent.futures
import requests
import threading
import time



thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def get_rxnorm_info(url_mod):
    session = get_session()
    
    url = url_mod.split("|")[0]
    rxcui= url_mod.split("|")[1]

    lst=[]
    
    with session.get(url) as response:
        response=response.json()
        
        try:
            for item in response['allRelatedGroup']['conceptGroup']:
                if item['tty'] == 'IN':
                    for ing in item['conceptProperties']:
                        lst.append((ing['rxcui'], ing['name']))
            return (rxcui,lst)
            
                        
        except:
            return (rxcui,"NOT_CURRENT_CONCEPT")
        
        
        
def get_all_rxnorm_info(sites):
    
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        completed = executor.map(get_rxnorm_info, sites)
        res.extend(completed)
        return res

      

if __name__ == "__main__":
    
    filename="rxnorm_2022.tsv"  

    df = pd.read_csv(filename,skiprows=1,names=['rxcui'])
    
    rxcuis = list(df['rxcui'].unique())
    
    print("total unique cuis",len(rxcuis))   
    batch=100
    print("batch_size",batch)   
    
    for i in range(0, len(rxcuis), batch):

        sub_rxcuis = rxcuis[i:i + batch]
        
        rxcui_urls = ["https://rxnav.nlm.nih.gov/REST/rxcui/" +str(rxcui)+"/allrelated.json"+"|"+str(rxcui) for rxcui in sub_rxcuis]
     
        
        res = get_all_rxnorm_info(rxcui_urls)    
      
       
        res_df= pd.DataFrame(res,columns=['rxcui','IN'])
        print(res_df)
        
        res_df.to_csv("rxcui_with_IN_ingredients_threading.csv",index=None,mode='a',header=False)
   
       



  
    
  
    
  