import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, SmallInteger
from sqlalchemy import inspect

metadata = MetaData()
households_table = Table('pumf_hh', metadata,
                         Column('HouseholdId', Integer, primary_key=True),
                         Column('puma', Integer),
                         Column('DwellingType', SmallInteger),
                         Column('NumberOfPersons', SmallInteger),
                         Column('Vehicles', SmallInteger),
                         Column('IncomeClass', Integer),
                         Column('weight', Integer),
                         )

persons_table = Table('pumf_person', metadata,
                      Column('HouseholdId', Integer, primary_key=True),
                      Column('puma', Integer),
                      Column('PersonNumber', SmallInteger),
                      Column('Age', Integer),
                      Column('Sex', Integer),
                      Column('License', Integer),
                      Column('EmploymentStatus', SmallInteger),
                      Column('Occupation', SmallInteger),
                      Column('StudentStatus', Integer),
                      Column('EmploymentStatus', Integer),
                      Column('weight', Integer),
                      )
