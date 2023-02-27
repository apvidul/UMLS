import os
import pandas as pd

# Intital Setup to get the required data.
# Once, rxn_tty and  rxn2_rela_rxn1 has been created the following section can be commented out:

# Loading RXNCONSO

conso_cols = [
    "rxcui",
    "lat",
    "ts",
    "lui",
    "stt",
    "sui",
    "ispref",
    "rxaui",
    "saui",
    "scui",
    "sdui",
    "sab",
    "tty",
    "code",
    "str",
    "srl",
    "suppress",
    "cvf",
]
rxnconso = pd.read_csv(
    os.path.join(os.getcwd(), "RxNorm_full_02062023", "rrf", "RXNCONSO.RRF"),
    sep="|",
    names=conso_cols,
    index_col=False,
    dtype=str,
)
print(rxnconso)


# Getting unique tty for every rxnorm
rxn_tty = rxnconso[["rxcui", "tty"]].copy().drop_duplicates()
print(rxn_tty)
rxn_tty.to_csv("rxn_tty.csv", index=None)


# Loading RXNREL
rel_cols = [
    "rxcui1",
    "rxaui1",
    "stype1",
    "rel",
    "rxcui2",
    "rxaui2",
    "stype2",
    "rela",
    "rui",
    "srui",
    "sab",
    "sl",
    "rg",
    "dir",
    "suppress",
    "cvf",
]
rxnrel = pd.read_csv(
    os.path.join(os.getcwd(), "RxNorm_full_02062023", "rrf", "RXNREL.RRF"),
    sep="|",
    names=rel_cols,
    index_col=False,
    dtype=str,
)
print(rxnrel)
rxn2_rela_rxn1 = rxnrel[["rxcui2", "rxcui1", "rela"]].drop_duplicates().dropna()
print(rxn2_rela_rxn1)
rxn2_rela_rxn1.to_csv("rxn2_rela_rxn1.csv", index=None)


# ---------------------------------------------------Extracting Ingredient Mappings--------------------------------------------------------


rxn_tty1 = pd.read_csv("rxn_tty.csv", skiprows=1, names=["rxcui1", "tty1"], dtype=str)
rxn_tty2 = pd.read_csv("rxn_tty.csv", skiprows=1, names=["rxcui2", "tty2"], dtype=str)
rxn2_rela_rxn1 = pd.read_csv("rxn2_rela_rxn1.csv", dtype=str)
print(rxn2_rela_rxn1)


full_rela = rxn2_rela_rxn1.merge(rxn_tty2, on="rxcui2").merge(rxn_tty1, on="rxcui1")
print(full_rela)


# Pulling mappings for TTY=IN / Bascially ingredient mapings: https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix1.html
"""
We define a concept called depth. Depth is the level of mappings we need to get to ingredient mapping
We first create the depth 0 ingredient mappings. This wll be the foundation of mappings that we will use moving on for depth 1 and depth 2 mappings. 
For Eg: TTY=SBD mappings are depth 1 mappings 

ttys mapped to ing till this step: None
ttys mapped in this step: 'BN','MIN','PIN','SCDC','SCDF','SCDG'
"""

in_relas = ["has_tradename", "part_of", "has_form", "ingredient_of"]
in_relas_tty1 = ["BN", "MIN", "PIN", "SCDC", "SCDF", "SCDG"]

in_relas_full = full_rela[
    (full_rela["rela"].isin(in_relas))
    & (full_rela["tty2"] == "IN")
    & (full_rela["tty1"].isin(in_relas_tty1))
]

# in_relas_full will have duplicates rows containing 'rxcui2' (ingredient rxnorm) and 'rxcui1' (some rxnorm) because same rxcui1 can have multiple tty's
in_relas_unq = in_relas_full[["rxcui2", "rxcui1"]].drop_duplicates()
print(in_relas_unq)
in_relas_unq = in_relas_unq.rename(columns={"rxcui1": "rxcui", "rxcui2": "ing_rxcui"})
print(in_relas_unq)
# in_relas_unq. We can also refer this to as depth 0 level rxcui-rxcui_ingredient mapping


# Pulling mappings for TTY=SBD
"""
TESTING
#sbdc = set(full_rela[(full_rela['rela']=='consists_of') & (full_rela['tty2']=='SBD') & (full_rela['tty1']=='SBDC')] ['rxcui2'].unique()) 
#bn = set(full_rela[(full_rela['rela']=='has_ingredient') & (full_rela['tty2']=='SBD') & (full_rela['tty1']=='BN')] ['rxcui2'].unique())   
#sbdc - bn and bn-sbdc gave 0 results. Meanining either one of them is enough to get full ingredients

Tragectory: 
we have the following mappings
sbd->bn (from the has_ingredinet relationship). We can also use sbd->sbdc (using the consists_of relationship).Both will give same result
bn->ing (from previous section)

sbd->bn->ing

ttys mapped to ing till this step: 'BN','MIN','PIN','SCDC','SCDF','SCDG'
ttys mapped in this step: 'SBD'
"""

sbd_relas_bn = full_rela[
    (full_rela["rela"] == "has_ingredient")
    & (full_rela["tty2"] == "SBD")
    & (full_rela["tty1"] == "BN")
]
print(sbd_relas_bn)
sbd_relas_bn = sbd_relas_bn[["rxcui2", "rxcui1"]].drop_duplicates()
print(sbd_relas_bn)

# mapping w depth 0 rxcui-rxcui_ing mapping to map the sbd rxcui at depth1 to rxcui_ing
sbd_relas_ing = pd.merge(
    sbd_relas_bn, in_relas_unq, left_on="rxcui1", right_on="rxcui"
)[["rxcui2", "ing_rxcui"]]
sbd_relas_ing = sbd_relas_ing.rename(columns={"rxcui2": "rxcui"})


# -----------------------------------------------------MERGE ALL INGREDIENT MAPPINGS----------------------------------------------------
# mappings=[in_relas_unq,sbd_relas_ing]
