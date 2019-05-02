
# HouseholdId,PersonNumber,Age,Sex,License,
    # TransitPass,EmploymentStatus,Occupation,FreeParking,StudentStatus,EmploymentZone,SchoolZone,ExpansionFactor
drop table if exists GTA_PERSONS;

CREATE TABLE GTA_PERSONS(HouseholdId INT(11),
PersonNumber INT(11),
Age smallint,
Sex VARCHAR(1),
License VARCHAR(1),
TransitPass VARCHAR(1) DEFAULT 'O',
EmploymentStatus VARCHAR(1),
Occupation VARCHAR(1),
FreeParking VARCHAR(1) DEFAULT 'O',
StudentStatus VARCHAR(1),
EmploymentZone smallint DEFAULT 0,
SchoolZone smallint DEFAULT 0,
ExpansionFactor smallint)
as SELECT tempId as HouseholdId, PersonNumber, Age, Sex, License, TransitPass,
          EmploymentStatus, Occupation, StudentStatus,  FreeParking,  EmploymentZone,
        0 as SchoolZone, finalweight as ExpansionFactor
from synpop_person order by tempId asc ;

drop table if exists GTA_HOUSEHOLDS;

#HouseholdId,HouseholdZone,ExpansionFactor,NumberOfPersons,DwellingType,Vehicles,IncomeClass
CREATE TABLE GTA_HOUSEHOLDS(
HouseholdId INT(11),
HouseholdZone SMALLINT,
ExpansionFactor SMALLINT,
NumberOfPersons TINYINT,
DwellingType TINYINT,
Vehicles TINYINT,
IncomeClass TINYINT) AS SELECT tempId as HouseholdId, maz as HouseholdZone, finalweight as ExpansionFactor, DwellingType,NumberOfPersons,Vehicles,IncomeClass from synpop_hh
order by tempId asc;



drop table if exists zonal_totals;

CREATE TABLE ZONAL_TOTALS(
    Zone SMALLINT NOT NULL DEFAULT 0,
    ExpandedHouseholds INT NOT NULL DEFAULT 0
)
AS (SELECT  HouseholdZone as Zone, SUM(ExpansionFactor) as ExpandedHouseholds from GTA_HOUSEHOLDS group by HouseholdZone)