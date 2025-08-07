configure_args=$(cat ../src/Makefile.global | grep "configure_args" | awk -F "configure_args =" '{ print $NF }')
echo "configure_args:"${configure_args}

declare MAKE_JOBS=$(($(cat /proc/cpuinfo | grep processor | wc -l) / 1))

prefix_args=$( cat ../src/Makefile.global | grep "configure_args" | awk -F "configure_args =" '{ print $NF }' | sed "s/'//g" | awk -F "prefix=" '{ print $NF }' | awk '{ print $1 }' )
echo "prefix_args:"${prefix_args}
cd opentenbase_ora_package_function
echo 'make USE_PGXS=1 ${prefix_args}/bin/pg_config clean'
make PG_CONFIG=${prefix_args}/bin/pg_config clean
 if [ $? -eq 0 ];then
            echo "=== opentenbase_ora_package_function_compile clean finished ==="
        else
            echo "=== opentenbase_ora_package_function_compile clean error($?) ==="
            exit 1
 fi
make PG_CONFIG=${prefix_args}/bin/pg_config -j${MAKE_JOBS}
 if [ $? -eq 0 ];then
            echo "=== opentenbase_ora_package_function_compile make finished ==="
        else
            echo "=== opentenbase_ora_package_function_compile make error($?) ==="
            exit 1
 fi
make PG_CONFIG=${prefix_args}/bin/pg_config install -j${MAKE_JOBS}
 if [ $? -eq 0 ];then
            echo "=== opentenbase_ora_package_function_compile make install finished ==="
        else
            echo "=== opentenbase_ora_package_function_compile make install error($?) ==="
            exit 1
 fi
echo "=== opentenbase_ora_package_function_compile compile is success  ==="
            
