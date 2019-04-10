
# HouseholdId,PersonNumber,Age,Sex,License,
    # TransitPass,EmploymentStatus,Occupation,FreeParking,StudentStatus,EmploymentZone,SchoolZone,ExpansionFactor
drop table if exists GTA_PERSONS;

CREATE TABLE GTA_PERSONS(HouseholdId INT(11),
PersonNumber INT(11),
Age Int,
Sex VARCHAR(1),
License VARCHAR(1),
TransitPass VARCHAR(1) DEFAULT '0',
EmploymentStatus VARCHAR(1),
Occupation VARCHAR(1),
FreeParking VARCHAR(1) DEFAULT '0',
StudentStatus VARCHAR(1),
EmploymentZone INT DEFAULT 0,
SchoolZone INT DEFAULT  0,
ExpansionFactor INT )
as SELECT tempId as HouseholdId, PersonNumber, Age, Sex, License, '0' as TransitPass,
          EmploymentStatus, Occupation, StudentStatus, '0' as FreeParking,  EmploymentZone,
          '0' as SchoolZone, finalweight as ExpansionFactor
from synpop_person order by tempId asc ;

drop table if exists GTA_HOUSEHOLDS;

#HouseholdId,HouseholdZone,ExpansionFactor,NumberOfPersons,DwellingType,Vehicles,IncomeClass
CREATE TABLE GTA_HOUSEHOLDS(
HouseholdId INT(11),
HouseholdZone INT(11),
ExpansionFactor INT (11),
NumberOfPersons INT(11),
DwellingType INT(11),
Vehicles INT(11),
IncomeClass INT(11)) AS SELECT tempId as HouseholdId, maz as HouseholdZone, finalweight as ExpansionFactor, DwellingType,NumberOfPersons,Vehicles,IncomeClass from synpop_hh
order by tempId asc;


