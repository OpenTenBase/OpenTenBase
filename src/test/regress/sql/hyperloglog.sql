--
-- HYPERLOGLOG
--

SELECT approx_count_distinct(tenthous) from onek;

SELECT approx_count_distinct(fivethous + tenthous) from onek;