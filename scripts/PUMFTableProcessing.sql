
USE GTAModelPopSyn;

/*###################################################################################################*/
--									PROCESSING GENERAL POPULATION
/*###################################################################################################*/

SELECT 'Completed processing seed person table...';

CREATE TABLE hhtable LIKE pumf_hh;
CREATE TABLE perstable LIKE pumf_person;

INSERT INTO hhtable 
SELECT * FROM pumf_hh;

INSERT INTO perstable 
SELECT * FROM pumf_person;

SELECT 'Created seed tables for general population...';

-- Generate unique ID to be used in PopSYnIII
ALTER TABLE hhtable
	ADD COLUMN hhnum INT NOT NULL AUTO_INCREMENT UNIQUE;
	
ALTER TABLE perstable	
	ADD COLUMN hhnum INT NULL;

UPDATE perstable a
LEFT JOIN hhtable b ON
	a.EstimationHouseholdId=b.EstimationHouseholdId 
	SET a.hhnum=b.hhnum;	

