CREATE TABLE EMPLOYEE
(
    ID serial,
    NAME varchar(100) NOT NULL,
	SALARY numeric(15, 2) NOT NULL,
	CREATED_DATE timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID)
);

CREATE OR REPLACE FUNCTION hello(p1 TEXT) RETURNS TEXT AS $$ BEGIN RETURN 'hello ' || p1; END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getUsers(mycurs OUT refcursor) RETURNS refcursor AS $$ BEGIN OPEN mycurs FOR select * from pg_user; END; $$ LANGUAGE plpgsql;