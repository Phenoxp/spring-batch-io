create table customer (
  id integer not null AUTO_INCREMENT,
  firstName varchar(255),
  lastName varchar(255),
  birthDate varchar(255),
  primary key(id)
) ENGINE=InnoDB;