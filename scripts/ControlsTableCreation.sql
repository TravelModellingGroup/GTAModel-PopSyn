# //Setting up MAZ, TAZ and META control tables for Toronto PopSynIII
# Binny M Paul, paulbm@pbworld.com Feb 2015 (original))
# Updated by Brendan T. Relly using TTS Data

USE GTAModelPopSyn;

-- Removing existing tables from previous runs
DROP TABLE IF EXISTS control_totals_maz,
    control_totals_taz,
    control_totals_meta;


/*###################################################################################################*/
--							  CREATING INITIAL TABLES
/*###################################################################################################*/

-- Create MAZ data table
CREATE TABLE control_totals_maz
(
    region    INT NOT NULL,
    puma      INT NOT NULL,
    taz       INT NOT NULL,
    maz       INT NOT NULL,
    tothh     INT NULL,
    totpop    INT NULL,
    s_O       INT NULL,
    s_S       INT NULL,
    s_P       INT NULL,
    license_Y INT NULL,
    license_N INT NULL,
    e_O       INT NULL,
    e_F       INT NULL,
    e_P       INT NULL,
    e_J       INT NULL,
    e_H       INT NULL,
    P         INT NULL,
    G         INT NULL,
    S         INT NULL,
    M         INT NULL,
    age0_14   INT NULL,
    age15_29  INT NULL,
    age30_44  INT NULL,
    age45_64  INT NULL,
    age65p    INT NULL,
    hhsize1   INT NULL,
    hhsize2   INT NULL,
    hhsize3   INT NULL,
    hhsize4p  INT NULL,
    numv1     INT NULL,
    numv2     INT NULL,
    numv3p    INT NULL,
    income_class_1 INT NULL,
    income_class_2 INT NULL,
    income_class_3 INT NULL,
    income_class_4 INT NULL,
    income_class_5 INT NULL,
    income_class_6 INT NULL,
    male      INT NULL,
    female    INT NULL,
    PRIMARY KEY (maz)
);

-- Loading TAZ data table
CREATE TABLE control_totals_taz
(
    region         INT NOT NULL,
    puma           INT NOT NULL,
    taz            INT NOT NULL,
    tothh          INT NULL,
    totpop         INT NULL,
    s_O            INT NULL,
    s_S            INT NULL,
    s_P            INT NULL,
    license_Y      INT NULL,
    license_N      INT NULL,
    e_O            INT NULL,
    e_F            INT NULL,
    e_P            INT NULL,
    e_J            INT NULL,
    e_H            INT NULL,
    P              INT NULL,
    G              INT NULL,
    S              INT NULL,
    M              INT NULL,
    age0_14        INT NULL,
    age15_29       INT NULL,
    age30_44       INT NULL,
    age45_64       INT NULL,
    age65p         INT NULL,
    hhsize1        INT NULL,
    hhsize2        INT NULL,
    hhsize3        INT NULL,
    hhsize4p       INT NULL,
    numv1          INT NULL,
    numv2          INT NULL,
    numv3p         INT NULL,
    income_class_1 INT NULL,
    income_class_2 INT NULL,
    income_class_3 INT NULL,
    income_class_4 INT NULL,
    income_class_5 INT NULL,
    income_class_6 INT NULL,
    male           INT NULL,
    female         INT NULL,
    PRIMARY KEY (taz)
);

-- Create meta controls table
CREATE TABLE control_totals_meta
(
    region INT NOT NULL,
    tothh  INT NOT NULL,
    totpop INT NOT NULL,
    P      INT NULL,
    G      INT NULL,
    S      INT NULL,
    M      INT NULL,
    income_class_1 INT NULL,
    income_class_2 INT NULL,
    income_class_3 INT NULL,
    income_class_4 INT NULL,
    income_class_5 INT NULL,
    income_class_6 INT NULL,
    PRIMARY KEY (region)
);

SELECT 'Created initial control tables...';
