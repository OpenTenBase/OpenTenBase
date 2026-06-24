pushd ./base
docker build -t opentenbasebase:1.0.0 .
popd

pushd ./host
docker build -t opentenbase:1.0.0 .
popd
