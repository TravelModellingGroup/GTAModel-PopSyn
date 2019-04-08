# Setting up MAZ, TAZ and META control tables for Toronto PopSynIII
# Binny M Paul, paulbm@pbworld.com Feb 2015
# --------------------------------------------------------------------
USE GTAModelPopSyn;

ALTER TABLE control_totals_maz	DROP PRIMARY KEY, ADD CONSTRAINT PK PRIMARY KEY (REGION, PUMA, TAZ, MAZ);

ALTER TABLE control_totals_taz	DROP PRIMARY KEY, ADD PRIMARY KEY (TAZ);

ALTER TABLE control_totals_meta	DROP PRIMARY KEY, ADD PRIMARY KEY (region);

