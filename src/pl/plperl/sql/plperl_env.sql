--
-- Test the environment setting
--
/*
PG_FUNCTION_INFO_V1(get_environ);

Datum
get_environ(PG_FUNCTION_ARGS)
{
	extern char **environ;
	int			nvals = 0;
	ArrayType  *result;
	Datum	   *env;

	for (char **s = environ; *s; s++)
		nvals++;

	env = palloc(nvals * sizeof(Datum));

	for (int i = 0; i < nvals; i++)
		env[i] = CStringGetTextDatum(environ[i]);

	result = construct_array(env, nvals, TEXTOID, -1, false, 'i');

	PG_RETURN_POINTER(result);
}
*/

-- directory path and dlsuffix are passed to us in environment variables
\getenv libdir PG_LIBDIR
\getenv dlsuffix PG_DLSUFFIX

\set regresslib :libdir '/regress' :dlsuffix

CREATE FUNCTION get_environ()
   RETURNS text[]
   AS :'regresslib', 'get_environ'
   LANGUAGE C STRICT;

-- fetch the process environment

CREATE FUNCTION process_env () RETURNS text[]
LANGUAGE plpgsql AS
$$

declare
   res text[];
   tmp text[];
   f record;
begin
    for f in select unnest(get_environ()) as t loop
         tmp := regexp_split_to_array(f.t, '=');
         if array_length(tmp, 1) = 2 then
            res := res || tmp;
         end if;
    end loop;
    return res;
end

$$;

-- plperl should not be able to affect the process environment

DO
$$
   $ENV{TEST_PLPERL_ENV_FOO} = "shouldfail";
   untie %ENV;
   $ENV{TEST_PLPERL_ENV_FOO} = "testval";
   my $penv = spi_exec_query("select unnest(process_env()) as pe");
   my %received;
   for (my $f = 0; $f < $penv->{processed}; $f += 2)
   {
      my $k = $penv->{rows}[$f]->{pe};
      my $v = $penv->{rows}[$f+1]->{pe};
      $received{$k} = $v;
   }
   unless (exists $received{TEST_PLPERL_ENV_FOO})
   {
      elog(NOTICE, "environ unaffected")
   }

$$ LANGUAGE plperl;

-- clean up to simplify cross-version upgrade testing
DROP FUNCTION get_environ();
