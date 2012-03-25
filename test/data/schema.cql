DROP COLUMNFAMILY User;
CREATE COLUMNFAMILY User (KEY varchar PRIMARY KEY, id varchar, city varchar, state varchar, zip varchar, e164phone varchar);
CREATE INDEX city_idx ON User(city);
CREATE INDEX state_idx ON User(state);
CREATE INDEX zip_idx ON User(zip);
CREATE INDEX e164phone_idx ON User(e164phone);
