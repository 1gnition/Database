CREATE TABLE persons (
		id integer(7),
		age integer(3),
		workclass varchar(20),
		education varchar(15),
		educationnum integer(3),
		maritalstatus varchar(25),
		occupation varchar(20),
		race varchar(20),
		sex varchar(10),
		capitalgain integer(8),
		capitalloss integer(8),
		country varchar(30),
		PRIMARY KEY (id)
);

CREATE TABLE marriage (
		id_person integer(7),
		id_relative integer(7),
		relationship varchar(9),
		PRIMARY KEY (id_person, id_relative),
		FOREIGN KEY (id_person) REFERENCES persons (id)
								ON DELETE CASCADE
								ON UPDATE CASCADE,
		FOREIGN KEY (id_relative) REFERENCES persons (id)
								  ON DELETE CASCADE
								  ON UPDATE CASCADE
);

CREATE TABLE cars (
		id integer(7),
		manufacturer varchar(10),
		model varchar(9),
		prod_year integer(4),
		PRIMARY KEY (id)
);

CREATE TABLE cars_owned (
		id_person integer(7),
		id_car integer(7),
		color varchar(10),
		purchase_date varchar(20),
		PRIMARY KEY (id_person, id_car),
		FOREIGN KEY (id_person) REFERENCES persons (id)
								ON DELETE CASCADE
								ON UPDATE CASCADE,
		FOREIGN KEY (id_car) REFERENCES cars (id)
							 ON DELETE CASCADE
							 ON UPDATE CASCADE
);