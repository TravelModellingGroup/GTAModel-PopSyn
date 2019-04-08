
USE GTAModelPopSyn;

# -- Setting up PUMF Tables for Toronto PopSynIII
#-- Binny M Paul, paulbm@pbworld.com, Nov2014

DROP TABLE IF EXISTS pumf_hh,
pumf_person,
hhtable,
perstable;

DROP TEMPORARY TABLE IF EXISTS tempHH,
tempPer,
tempHH2,
tempPer2,
Numbers;

/*###################################################################################################*/
--									SETTING UP PUMF DATABASE
/*###################################################################################################*/

CREATE TABLE pumf_hh(
	EstimationHouseholdId INT NOT NULL,
	DwellingType INT NULL,
	NumberOfPersons INT NULL,
	Vehicles INT NULL,
	IncomeClass INT NULL,
	weight INT NULL,
	puma INT NULL,
	PRIMARY KEY(EstimationHouseholdId)
);


CREATE TABLE pumf_person(
	EstimationHouseholdId INT NOT NULL,
	PersonNumber INT NOT NULL,
	Age INT NULL,
	Sex VARCHAR(1) NULL,
	License VARCHAR(1) NULL,
	EmploymentStatus VARCHAR(1) NULL,
	Occupation VARCHAR(1) NULL,
	StudentStatus VARCHAR(1) NULL,
	weight INT NULL,
	puma INT NULL,
	CONSTRAINT PK PRIMARY KEY(PersonNumber, EstimationHouseholdId)
);

