import pandas as pd
import matplotlib as plot
import matplotlib.pyplot as plt

zones = pd.read_csv("Zones.csv")[["Zone#", "PD"]]
households = pd.read_csv("gtamodelpopsyn_synpop_hh.csv")[
    ["taz", "EstimationHouseholdId", "DwellingType", "NumberOfPersons", "Vehicles", "IncomeClass", "finalweight"]]

#persons = pd.read_csv("pp.csv",dtype={'PersonNumber':int,'finalweight':int,'Age':int},
 #                     low_memory=False)[
#    ["taz", "finalweight", "PersonNumber", "Age", "Sex", "EmploymentStatus", "Occupation", "StudentStatus"]]

maz = pd.read_csv("gtamodel_maz.csv")

households.rename(columns={'finalweight': 'ExpansionFactor', "taz": "Zone"}, inplace=True)
#persons.rename(columns={'finalweight': 'ExpansionFactor', "taz": "Zone"}, inplace=True)

print(households.shape)

householdspd = pd.merge(households, zones, left_on="Zone", right_on="Zone#")

# pp = pd.merge(persons, zones, left_on="Zone", right_on="Zone#")

maz = pd.merge(maz, zones, left_on="taz", right_on="Zone#")

mazpd = maz.groupby("PD")

# pppd = pp.groupby("PD")

# print(mazpd.totalhh.sum())

# print(householdspd.groupby("PD").ExpansionFactor.sum())

# group by PD and sum tot pop



# pdhh.set_index(['Control', 'Synthesized'], inplace=True)


pdhh = pd.concat([mazpd.numv1.sum(),
                  householdspd[householdspd.Vehicles == 1].groupby("PD").ExpansionFactor.sum()

                ], axis=1
                )

print(pdhh)

pdhh.to_csv("veh_compare2.csv")

print(households.shape)

householdspd.to_csv("hh_with_pd.csv", index=False)
