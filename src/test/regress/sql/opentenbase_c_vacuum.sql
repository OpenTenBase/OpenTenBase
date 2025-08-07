-- schema and table name with special character
CREATE SCHEMA "S-test";
CREATE TABLE "eg_sync-cc0vf52i" (id int);
CREATE TABLE "2ac" (id int);
CREATE TABLE "ASD2" (id int);
CREATE TABLE "S-test"."2ac" (id int);
ANALYZE "eg_sync-cc0vf52i";
ANALYZE public."eg_sync-cc0vf52i";
ANALYZE "public"."eg_sync-cc0vf52i";
ANALYZE "2ac";
ANALYZE "ASD2";
ANALYZE "S-test"."2ac";
DROP TABLE "eg_sync-cc0vf52i","2ac","ASD2","S-test"."2ac";
DROP SCHEMA "S-test";
