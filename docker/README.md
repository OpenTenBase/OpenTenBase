# Docker

This directory holds two independent image build paths.

## KubeBlocks image (`docker/k8s_support/`)

This Dockerfile is only used to build the image for KubeBlocks. It only contains the necessary OpenTenBase binaries, but no entrypoint or command to run the OpenTenBase cluster.

To build and push the image, run the following command in the root directory of the repository:

```bash
make push-image
```

NOTE:
- The image is pushed to the `damainlau/opentenbase` repository, should change it before merge to the main branch.

## 1C2D example images (`docker/buildImage.sh`)

`docker/buildImage.sh` builds the two images used by the "Build Cluster (1C2D) using docker"
guide. It builds `docker/base` and then `docker/host`:

| Image | Dockerfile | Contents |
| --- | --- | --- |
| `opentenbasebase:1.0.0` | `docker/base/Dockerfile` | Ubuntu 20.04, the `opentenbase` OS user, `openssh-server` and a pre-generated SSH key pair. No OpenTenBase binaries. |
| `opentenbase:1.0.0` | `docker/host/Dockerfile` | Ubuntu 20.04 plus a full OpenTenBase source build (cloned from GitHub during the image build), and `docker/host/copy-ssh-keys` installed as `copy-ssh-keys`. |

### `${SOURCECODE_PATH}`

`${SOURCECODE_PATH}` is not defined by any script in this directory. It is a shell variable
that the user exports, and it means **the absolute path of your local clone of this
repository**:

- `README.md` (root, section "Building") sets it with
  `export SOURCECODE_PATH=/data/opentenbase/OpenTenBase`.
- `docker/host/Dockerfile` bakes the same path into the `opentenbase` image with
  `ENV SOURCECODE_PATH=/data/opentenbase/OpenTenBase`.

`/data/opentenbase/OpenTenBase` is only the value used by the project's own examples. Replace
it with the directory where **you** cloned this repository before running the commands below.

### Annotated commands

```bash
export SOURCECODE_PATH=/path/to/your/opentenbase/clone   # 1
cd ${SOURCECODE_PATH}/docker                            # 2
./buildImage.sh                                         # 3
cd ${SOURCECODE_PATH}/example/1c_2d_cluster             # 4
docker-compose up -d                                    # 5
docker-compose exec opentenbaseCN /bin/bash             # 6
cp ~/pgxc_conf/pgxc_ctl.conf ~/pgxc_ctl/                # 7
pgxc_ctl                                                # 8
```

| # | Purpose | Working directory | Prerequisites | Value you must replace | Example |
| --- | --- | --- | --- | --- | --- |
| 1 | Point the shell at your source tree so the `${SOURCECODE_PATH}` expansions below resolve | any | this repository is already cloned locally | the entire value — it must be your clone path | `export SOURCECODE_PATH=/data/opentenbase/OpenTenBase` |
| 2 | Enter the directory that holds `buildImage.sh` | any | step 1 | nothing | `cd ${SOURCECODE_PATH}/docker` |
| 3 | Build `opentenbasebase:1.0.0` and then `opentenbase:1.0.0` | `${SOURCECODE_PATH}/docker` | a Docker engine, and outbound network access — `docker/host/Dockerfile` runs `apt-get` and `git clone https://github.com/OpenTenBase/OpenTenBase` at build time | nothing | `./buildImage.sh` |
| 4 | Enter the Compose example that brings up the 1C2D service | any | **the directory must exist — it does not on `master`; see "Status of `example/1c_2d_cluster`" below** | — | — |
| 5 | Start the `opentenbaseCN`, `opentenbaseDN1` and `opentenbaseDN2` containers | `example/1c_2d_cluster` | a `docker-compose.yaml` in that directory (absent on `master`) | — | — |
| 6 | Open a shell inside the CN container | `example/1c_2d_cluster` | step 5 | nothing | `docker-compose exec opentenbaseCN /bin/bash` |
| 7 | Put the cluster configuration where `pgxc_ctl` looks for it | inside the CN container, as user `opentenbase` | `~/pgxc_conf/pgxc_ctl.conf` must exist — it was provided by the Compose file's `./pgxc_conf/cn/` volume mount | the source path, if you supply the configuration yourself | `mkdir -p ~/pgxc_ctl && cp ~/pgxc_conf/pgxc_ctl.conf ~/pgxc_ctl/` |
| 8 | Run the cluster tool; at its prompt issue `deploy all` and then `init all` | `~/pgxc_ctl` | step 7 | `IP_1` / `IP_2` and the node directories inside `pgxc_ctl.conf` | `IP_1=172.16.200.10`, `IP_2=172.16.200.15` |

Note for step 7: `pgxc_ctl` resolves its configuration as
`<pgxc_ctl_home>/<configFile>`, where `pgxc_ctl_home` defaults to `$HOME/pgxc_ctl` and
`configFile` defaults to `pgxc_ctl.conf`
(`contrib/pgxc_ctl/pgxc_ctl.c`, `build_configuration_path()` and
`setDefaultIfNeeded(VAR_configFile, "pgxc_ctl.conf")`;
`contrib/pgxc_ctl/pgxc_ctl.h`, `#define DEFAULT_CONF_FILE_NAME "pgxc_ctl.conf"`).
The file name used above is therefore correct as written and must not be changed — see
"`pgxc_ctl.conf` is not renamed to `pgxc.conf`" below.

### Status of `example/1c_2d_cluster`

Steps 4 through 6 cannot run against the current `master`. The whole `example/` directory was
removed by commit
[`aca7e2c`](https://github.com/OpenTenBase/OpenTenBase/commit/aca7e2c34a25e1480548a20dc7462612fb8a17f7)
("Update basecode version from 2.6.0 to 5.0.0", 2025-08-07), which deleted
`example/1c_2d_cluster/docker-compose.yaml`, `example/1c_2d_cluster/README` and
`example/1c_2d_cluster/pgxc_conf/cn/pgxc_ctl.conf`. `git ls-tree -r --name-only HEAD`
contains no path matching `1c_2d`.

Removed along with those files, and needed by steps 4 to 7:

- `docker-compose.yaml` — the file that created the containers, the bridge network
  `OpenTenBaseNetWork` on `172.16.200.0/26`, and the addresses used by the guide
  (CN `172.16.200.5`, DN1 `172.16.200.10`, DN2 `172.16.200.15`);
- the `./pgxc_conf/cn/` directory that the Compose file mounted at
  `/home/opentenbase/pgxc_conf`, which is where step 7's `~/pgxc_conf/pgxc_ctl.conf` came
  from.

What survives on `master` is the image build only (steps 1 to 3). The 1C2D configuration
template itself is still published with the documentation, as
`docs/guide/pgxc_ctl_double.conf` in the
[`OpenTenBase/docs`](https://github.com/OpenTenBase/docs) repository. If you need a working
1C2D deployment today, use the Kubernetes/KubeBlocks path shipped in `docker/k8s_support/`
and its documentation guide, or supply your own `docker-compose.yaml` and `pgxc_conf/`
directory taken from commit `aca7e2c^`.

### `pgxc_ctl.conf` is not renamed to `pgxc.conf`

The published Docker 1C2D guide states that `deploy all` "will use `pgxc.conf` located in
`/home/$USER/pgxc_ctl/pgxc.conf`". No such rename happens anywhere. `pgxc_ctl` reads only
`pgxc_ctl.conf`:

- `contrib/pgxc_ctl/pgxc_ctl.h`: `#define DEFAULT_CONF_FILE_NAME "pgxc_ctl.conf"`;
- `contrib/pgxc_ctl/pgxc_ctl.c`: `setDefaultIfNeeded(VAR_configFile, "pgxc_ctl.conf");`, and
  `build_configuration_path()` resolves the file as `<pgxc_ctl_home>/<configFile>`, with
  `pgxc_ctl_home` defaulting to `$HOME/pgxc_ctl`.

`pgxc.conf` belongs to a different contrib tool, `pgxc_ddl`: `contrib/pgxc_ddl/pgxc_ddl` sets
`PGXC_CONF=$DATA_FOLDER/pgxc.conf`, and the only `pgxc.conf`-named file tracked in this
repository is `contrib/pgxc_ddl/pgxc.conf.sample`. The two names are unrelated, so the
configuration you copy in step 7 keeps the name `pgxc_ctl.conf`.
