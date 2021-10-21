####### PREV ################
CREATE TABLE prev.battalion(
  id_battalion varchar(10000),
  battalion varchar(10000), 
  station_area varchar(10000)
);

CREATE TABLE prev.district(
  id_district varchar(10000),
  neighborhood_district varchar(10000),
  city varchar(10000), 
  zipcode varchar(10000)
);

####### STG ################
CREATE TABLE stg.battalion(
  id_battalion varchar(10000),
  battalion varchar(10000), 
  station_area varchar(10000)
);

CREATE TABLE stg.district(
  id_district varchar(10000),
  neighborhood_district varchar(10000),
  city varchar(10000), 
  zipcode varchar(10000)
);

####### DWH ################
CREATE TABLE dwh.dim_battalion(
  sk_battalion bigint identity(1, 1),
  id_battalion varchar(10000),
  battalion varchar(10000), 
  station_area varchar(10000)
);

CREATE TABLE dwh.dim_district(
  sk_district bigint identity(1, 1),
  id_district varchar(10000),
  neighborhood_district varchar(10000),
  city varchar(10000), 
  zipcode varchar(10000)
);


CREATE TABLE dwh.dim_date(
  sk_date bigint identity(1, 1),
  date timestamp,
  year int,
  month int,
  quarter int,
  d_o_w varchar(100),
  d_o_m varchar(1000),
  d_o_y varchar(1000)
);



