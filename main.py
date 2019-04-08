import sys
import numpy as np;
import pandas as pd

# all original input files will remain intact, they will be processed into an input supported by the database

households_base = pd.read_csv("data/EstimationHouseholds.csv")
persons_base = pd.read_csv("data/EstimationPersons.csv")

households_base.rename(columns={'ExpansionFactor': 'weighth'}, inplace=True)
persons_base.rename(columns={'ExpansionFactor': 'weightp'}, inplace=True)
# create a new puma column for households and persons (popsyn3 requires this extra separation).
households_base['puma'] = 0

# re order the columns


households_base.sort_values(by=['HouseholdZone'], ascending=True).reset_index(inplace=True)

# <= 624 will be PUMA 0
# > 625 will be PUMA 1


households_base.loc[households_base.HouseholdZone <= 624, 'puma'] = 1
households_base.loc[households_base.HouseholdZone >= 625, 'puma'] = 2

households_base = households_base[['HouseholdId', 'DwellingType', 'NumberOfPersons', 'Vehicles',
                                   'IncomeClass', 'weighth', 'HouseholdZone', 'puma']]

# aassign a valid income value to '7' to 1-6


# households_base.loc[households_base.IncomeClass == 7] = np.random.randint(1, 6)

households_base.IncomeClass = households_base.IncomeClass.apply(lambda x: np.random.randint(1, 7) if 7 else x)

# we need to merge households and persons

persons_households = pd.merge(left=persons_base, right=households_base, left_on="HouseholdId", right_on="HouseholdId",
                              how="left")

print(persons_households.shape)
print(persons_base.shape)

persons_households.loc[persons_households.HouseholdZone <= 624, 'puma'] = 1
persons_households.loc[persons_households.HouseholdZone > 625, 'puma'] = 2

# persons_households= persons_households.drop_duplicates()
# persons_households.sort_values(by=['HouseholdZone', 'HouseholdId'], ascending=True, inplace=True)
persons_households.sort_values(by=['HouseholdId'], ascending=True).reset_index(inplace=True)
persons_households.to_csv("input/households_persons_merge.csv")

# print(persons_base.shape)

print(persons_households.shape)

# drop and re order columns

# create MAZ, TAZ, and META totals

gta_maz = pd.DataFrame(columns=['region',
                                'puma', 'taz', 'totalhh', 'totpop', 's=O', 's=S', 's=P', 'license=Y'
    , 'license=N', 'e=O', 'e=F', 'e=P', 'e=J', 'e=H', 'P', 'G', 'S', 'M', 'age0_14', 'age15_29', 'age30_44', 'age45_64'
    , 'age65p', 'hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p', 'numv1', 'numv2', 'numv3p',
                                'income_class_1',
                                'income_class_2',
                                'income_class_3',
                                'income_class_4',
                                'income_class_5',
                                'income_class_6',
                                'male',
                                'female'])


def sum_column(group, column, value, weight='weighth'):
    return group[group[column] == value][weight].sum()


def sum_column_gte(group, column, value, weight='weighth'):
    return group[group[column] == value][weight].sum()


def sum_column_range(group, column, value, value2, weight='weighth'):
    return group[(group[column] >= value) & (group[column] <= value2)][weight].sum()


persons_households.sort_values(by=['HouseholdZone'], ascending=True).reset_index()
households_base = households_base[households_base.HouseholdZone < 6000]
persons_households = persons_households[persons_households.HouseholdZone < 6000]
hh_group = persons_households.copy().groupby(['HouseholdZone'])

hh2_group = households_base.copy().groupby(['HouseholdZone'])

gta_maz['totalhh'] = hh2_group.weighth.sum().astype(int).to_list()
gta_maz['totpop'] = hh_group.weightp.sum().astype(int).to_list()

gta_maz.to_csv("input/gtamodel_taz.csv", index=False)

gta_maz['taz'] = hh_group.groups.keys()
gta_maz['male'] = hh_group.apply(lambda x: sum_column(x, 'Sex', 'M', 'weightp')).astype(int).to_list()
gta_maz['female'] = hh_group.apply(lambda x: sum_column(x, 'Sex', 'F', 'weightp')).astype(int).to_list()
gta_maz['income_class_1'] = hh2_group.apply(lambda x: sum_column(x, 'IncomeClass', 1, 'weighth')).astype(int).to_list()
gta_maz['income_class_2'] = hh2_group.apply(lambda x: sum_column(x, 'IncomeClass', 2, 'weighth')).astype(int).to_list()
gta_maz['income_class_3'] = hh2_group.apply(lambda x: sum_column(x, 'IncomeClass', 3, 'weighth')).astype(int).to_list()
gta_maz['income_class_4'] = hh2_group.apply(lambda x: sum_column(x, 'IncomeClass', 4, 'weighth')).astype(int).to_list()
gta_maz['income_class_5'] = hh2_group.apply(lambda x: sum_column(x, 'IncomeClass', 5, 'weighth')).astype(int).to_list()
gta_maz['income_class_6'] = hh2_group.apply(lambda x: sum_column(x, 'IncomeClass', 6, 'weighth')).astype(int).to_list()

gta_maz['numv1'] = hh2_group.apply(lambda x: sum_column(x, 'Vehicles', 1, 'weighth')).astype(int).to_list()
gta_maz['numv2'] = hh2_group.apply(lambda x: sum_column(x, 'Vehicles', 2, 'weighth')).astype(int).to_list()
gta_maz['numv3p'] = hh2_group.apply(lambda x: sum_column_gte(x, 'Vehicles', 3, 'weighth')).astype(int).to_list()

gta_maz['hhsize1'] = hh2_group.apply(lambda x: sum_column(x, 'NumberOfPersons', 1, 'weighth')).astype(int).to_list()
gta_maz['hhsize2'] = hh2_group.apply(lambda x: sum_column(x, 'NumberOfPersons', 2, 'weighth')).astype(int).to_list()
gta_maz['hhsize3'] = hh2_group.apply(lambda x: sum_column(x, 'NumberOfPersons', 3, 'weighth')).astype(int).to_list()
gta_maz['hhsize4p'] = hh2_group.apply(lambda x: sum_column_gte(x, 'NumberOfPersons', 4, 'weighth')).astype(
    int).to_list()

gta_maz['age0_14'] = hh_group.apply(lambda x: sum_column_range(x, 'Age', 0, 14, 'weightp')).astype(int).to_list()
gta_maz['age15_29'] = hh_group.apply(lambda x: sum_column_range(x, 'Age', 15, 29, 'weightp')).astype(int).to_list()
gta_maz['age30_44'] = hh_group.apply(lambda x: sum_column_range(x, 'Age', 30, 44, 'weightp')).astype(int).to_list()
gta_maz['age45_64'] = hh_group.apply(lambda x: sum_column_range(x, 'Age', 45, 64, 'weightp')).astype(int).to_list()
gta_maz['age65p'] = hh_group.apply(lambda x: sum_column_range(x, 'Age', 65, 2000, 'weightp')).astype(int).to_list()

gta_maz['e=J'] = hh_group.apply(lambda x: sum_column(x, 'EmploymentStatus', 'J', 'weightp')).astype(int).to_list()
gta_maz['e=P'] = hh_group.apply(lambda x: sum_column(x, 'EmploymentStatus', 'P', 'weightp')).astype(int).to_list()
gta_maz['e=F'] = hh_group.apply(lambda x: sum_column(x, 'EmploymentStatus', 'F', 'weightp')).astype(int).to_list()
gta_maz['e=O'] = hh_group.apply(lambda x: sum_column(x, 'EmploymentStatus', 'O', 'weightp')).astype(int).to_list()
gta_maz['e=H'] = hh_group.apply(lambda x: sum_column(x, 'EmploymentStatus', 'H', 'weightp')).astype(int).to_list()

gta_maz['P'] = hh_group.apply(lambda x: sum_column(x, 'Occupation', 'P', 'weightp')).astype(int).to_list()
gta_maz['G'] = hh_group.apply(lambda x: sum_column(x, 'Occupation', 'G', 'weightp')).astype(int).to_list()
gta_maz['S'] = hh_group.apply(lambda x: sum_column(x, 'Occupation', 'S', 'weightp')).astype(int).to_list()
gta_maz['M'] = hh_group.apply(lambda x: sum_column(x, 'Occupation', 'M', 'weightp')).astype(int).to_list()

gta_maz['license=Y'] = hh_group.apply(lambda x: sum_column(x, 'License', 'Y', 'weightp')).astype(int).to_list()
gta_maz['license=N'] = hh_group.apply(lambda x: sum_column(x, 'License', 'N', 'weightp')).astype(int).to_list()

gta_maz['s=O'] = hh_group.apply(lambda x: sum_column(x, 'StudentStatus', 'O', 'weightp')).astype(int).to_list()
gta_maz['s=S'] = hh_group.apply(lambda x: sum_column(x, 'StudentStatus', 'S', 'weightp')).astype(int).to_list()
gta_maz['s=P'] = hh_group.apply(lambda x: sum_column(x, 'StudentStatus', 'P', 'weightp')).astype(int).to_list()

gta_maz['totpop'] = hh_group.weightp.sum().astype(int).to_list()

gta_maz['totalhh'] = hh2_group.weighth.sum().astype(int).to_list()

gta_maz.loc[gta_maz.taz <= 624, 'puma'] = 1
gta_maz.loc[gta_maz.taz > 624, 'puma'] = 2
gta_maz['region'] = 1

gta_maz.to_csv("input/gtamodel_taz.csv", index=False)

gta_maz['maz'] = gta_maz['taz']

gta_maz = gta_maz[['region',
                   'puma', 'taz', 'maz', 'totalhh', 'totpop', 's=O', 's=S', 's=P', 'license=Y'
    , 'license=N', 'e=O', 'e=F', 'e=P', 'e=J', 'e=H', 'P', 'G', 'S', 'M', 'age0_14', 'age15_29', 'age30_44', 'age45_64'
    , 'age65p', 'hhsize1', 'hhsize2', 'hhsize3', 'hhsize4p', 'numv1', 'numv2', 'numv3p',
                   'income_class_1',
                   'income_class_2',
                   'income_class_3',
                   'income_class_4',
                   'income_class_5',
                   'income_class_6',
                   'male',
                   'female']]

gta_maz.to_csv("input/gtamodel_maz.csv", index=False)
# generate taz version

# creta meta totals

gta_meta = pd.DataFrame(columns=['region', 'totalhh', 'totpop',
                                 'P', 'G', 'S', 'M',
                                 'income_class_1',
                                 'income_class_2',
                                 'income_class_3',
                                 'income_class_4',
                                 'income_class_5',
                                 'income_class_6'

                                 ]);

gta_meta.loc[0] = [1,
                   gta_maz['totalhh'].sum(),
                   gta_maz['totpop'].sum(),
                   gta_maz['P'].sum(),
                   gta_maz['G'].sum(),
                   gta_maz['S'].sum(),
                   gta_maz['M'].sum(),
                   gta_maz['income_class_1'].sum(),
                   gta_maz['income_class_2'].sum(),
                   gta_maz['income_class_3'].sum(),
                   gta_maz['income_class_4'].sum(),
                   gta_maz['income_class_5'].sum(),
                   gta_maz['income_class_6'].sum()
                   ]

gta_meta.to_csv("input/gta_meta.csv", index=False)

# region', 'puma', 'taz', 'maz', 'totalhh', 'totpop', 's=O', 's=S', 's=P', 'license=Y'  'license=N', 'e=O', 'e=F', 'e=P', 'e=J',
# 'e=H', 'P', 'G', 'S', 'M', 'age0_14', 'age15_29', 'age30_44', 'age45_64'
persons_households.loc[persons_households.HouseholdZone <= 624, 'puma'] = 1
persons_households.loc[persons_households.HouseholdZone > 625, 'puma'] = 2

households_base.loc[households_base.HouseholdZone <= 624, 'puma'] = 1
households_base.loc[households_base.HouseholdZone > 625, 'puma'] = 2

persons_households = persons_households[['HouseholdId', 'PersonNumber', 'Age', 'Sex', 'License', 'EmploymentStatus',
                                         'Occupation', 'StudentStatus', 'weightp', 'puma', 'HouseholdZone']]
persons_households.rename(columns={'weightp': 'weight'}, inplace=True)
households_base = households_base[['HouseholdId', 'DwellingType', 'NumberOfPersons', 'Vehicles',
                                   'IncomeClass', 'weighth', 'puma']]

persons_households.to_csv("input/persons_processed.csv", index=False)
households_base.rename(columns={'weighth': 'weight'}, inplace=True)

households_base.to_csv("input/households_processed.csv", index=False)

sys.exit()
