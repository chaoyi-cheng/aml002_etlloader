/* misc script for testing */
select * from aml.etl_entity;
select * from aml.etl_transactions;

-- tables have to be dropped in the following order
drop table aml.etl_transactions;
drop table aml.etl_entity;