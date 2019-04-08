# Export output to CSVs
# Binny M Paul, paulbm@pbworld.com, Dec 2014
# ------------------------------------------------------------------------------------

USE GTAModelPopSyn;

-- Cleaning up objects created during previous SQL transactions

select * into outfile 'C:\\Users\\brendan\\Documents\\popsyn3-2016\\output/gtamodel_2016_synthesized_households.csv' fields terminated by ',' from synpop_hh;
select * into outfile 'C:\\Users\\brendan\\Documents\\popsyn3-2016\\output/gtamodel_2016_synthesized_persons.csv' fields terminated by ',' from synpop_person;