import pandas as pd
import requests



def get_ingredients(rxcui):
    base_url = "https://rxnav.nlm.nih.gov/REST/rxcui/" + \
        str(rxcui)+"/allrelated.json"
    lst = list()
    response = requests.get(base_url).json()

    try:

        for item in response['allRelatedGroup']['conceptGroup']:
            if item['tty'] == 'IN':
                for ing in item['conceptProperties']:
                    lst.append((ing['rxcui'], ing['name']))
        return lst
    except:
        print("Issue with RxNorm:",rxcui, "Check status of RxNorm at https://mor.nlm.nih.gov/RxNav")




def main():
    
    filename="missingRxNormsCUI.csv"    
    df = pd.read_csv(filename)
    
    print(df,"\n")
    df = df[df.columns[1:]]
    
    
    df['rxnorm'] = df['Code'].map(lambda x: x.split(":")[1])
    print(df,"\n")
    
    
    df['ingredients'] = df['rxnorm'].map(lambda x: get_ingredients(x))
    print(df)
    
    df.to_csv(filename.split(".")[0]+"_with_ingredients.csv",index=None)
    

if __name__ == "__main__":
    main()