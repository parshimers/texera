<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Kubernetes Deployment

Refer to https://github.com/Texera/texera/wiki/Install-Texera for details.

## Allocating computing units with Slurm

With `workflowComputingUnitManager.slurm.enabled`, users can choose **Slurm** as a computing-unit
type. The computing-unit manager submits each unit as a Slurm job through slurmrestd's unix socket,
and the gateway routes to the node Slurm chose.

```
browser -> gateway -> computing unit on a Slurm node, once Slurm reports the job running
             ^
             | the unit's recorded address
             |
computing-unit manager -- socket (hostPath) --> slurmrestd -> slurmctld -> job
```

### On the host

Run slurmrestd on the host itself, as the Slurm user computing units should run as, listening on a
socket in a directory of its own:

```bash
slurmrestd -a rest_auth/local -s openapi/slurmctld unix:/srv/slurmrestd/slurmrestd.socket
```

`rest_auth/local` decides who is calling from the socket's peer credentials, which sets two
requirements. A request answered `401` means one of them is unmet, and the manager logs which:

- The manager must run as slurmrestd's uid. Set `slurm.runAsUser` and `slurm.runAsGroup` to it.
- slurmrestd must not run in a container of its own, even with the right uid. In its own PID
  namespace it cannot see the pod's pid, and refuses it.

On **rootless k3s** a pod's uid is not the uid slurmrestd sees, because the pod runs in a user
namespace. With the usual `/etc/subuid` entry starting at 100000, uid `n` in a pod appears on the
host as `100000 + n - 1`, and only those uids and the uid running k3s can be matched at all. Either:

- Keep `runAsUser: 1001`, and run slurmrestd as host uid 101000. Jobs run as that uid, which must
  exist on the Slurm nodes.
- Set `runAsUser: 0` and `runAsGroup: 0`, and run slurmrestd as the user running k3s, which pod
  uid 0 maps to. Jobs run as that user. The manager is root only inside its user namespace.

Rootless k3s also shapes the rest of the setup:

- Pods see a copy of `/run`, `/var/lib` and `/etc` taken when k3s starts, so a socket directory
  created there later is missing for them (`hostPath type check failed`). The default
  `/srv/slurmrestd` avoids this.
- Pods reach the host at its real IP address, and only `LoadBalancer` services are forwarded to the
  host, so `slurm.clusterAddress` needs servicelb enabled. Without it the published services never
  get a port, and installs wait on an object store they cannot reach.
- A service port below 1024 gains 10000: the gateway's port 80 becomes 10080, which browsers refuse
  to open by default.

[`values-slurm-rootless.yaml`](values-slurm-rootless.yaml) is a working overlay for this setup, with
Slurm on the k3s host:

```bash
sed 's/192.0.2.10/<host address>/' bin/k8s/values-slurm-rootless.yaml > slurm.yaml
bin/k8s/install.sh -f slurm.yaml
```

### Using each user's Slurm token instead

If your cluster uses `rest_auth/jwt`, users can give a Slurm user name and token when they create a
unit, and its job runs as that user. Nothing in the chart changes. Leaving both blank uses
`rest_auth/local` as above.

In `slurm.conf`, with a key of 32 random bytes that only `SlurmUser` can read (Debian packages the
plugin separately, as `slurm-wlm-jwt-plugin`):

```
AuthAltTypes=auth/jwt
AuthAltParameters=jwt_key=/var/lib/slurm/ctld/jwt_hs256.key
```

Then run slurmrestd with only that plugin:

```bash
SLURM_JWT=daemon slurmrestd -a rest_auth/jwt -s openapi/slurmctld unix:/srv/slurmrestd/slurmrestd.socket
```

Do not also load `rest_auth/local`. It decides alone when both are loaded: a matching uid is accepted
with any token at all, even an invalid one, and the job runs as slurmrestd's uid, while any other uid
is refused even with a valid token.

With `rest_auth/jwt`, `runAsUser` no longer has to match slurmrestd's uid, and slurmrestd may run in
a container of its own, as long as the manager can open the socket. The user must exist on the Slurm
nodes and be able to read what `script` runs. A user gets a token with `scontrol token
lifespan=<seconds>`.

The token goes to the manager and stays in its memory. It is not written to the database, the job, or
any log, which has consequences:

- **A restart or an expired token.** When the manager restarts, or Slurm stops accepting a token, the
  unit's status becomes `Unknown` and it says why. The job keeps running and the gateway keeps routing
  to it, but the manager can no longer read or cancel it. Terminating the unit only stops tracking it,
  so the job runs on until someone uses `scancel` or it reaches `time_limit`. Make the token's lifespan
  at least `time_limit`, and expect upgrades that restart the manager to do this to every such unit.
- **One replica.** Tokens are not shared between pods, so keep `workflowComputingUnitManager.numOfPods`
  at 1.
- **An occupied address.** Until such a unit is terminated it keeps its address, because its job may
  still be there. Other units on that address stay `Pending`.
- **Changing modes.** A unit created without a token cannot be read or terminated by a slurmrestd that
  only has `rest_auth/jwt`. Terminate those units before switching.

The manager only ever uses a unit's own token on that unit's job. On Slurm 24.11.5, cancelling another
user's job through slurmrestd took 10 seconds to fail and then left slurmctld unresponsive to everyone,
so take care with any other client of this slurmrestd.

### Values

| Value (under `workflowComputingUnitManager.slurm`) | Meaning |
| --- | --- |
| `socketHostDir`, `socketFileName` | Where the socket is on the host. The directory is mapped in read-only, so a restarted slurmrestd's new socket is seen. |
| `nodeSelector` | Must select the host running slurmrestd. |
| `runAsUser`, `runAsGroup` | slurmrestd's uid and gid, which units' jobs run as. |
| `apiVersion` | The `v0.0.xx` slurmrestd serves: `slurmrestd -d list`. |
| `jobJson` | The body of `POST /slurm/<apiVersion>/job/submit`. |
| `unitAddressTemplate` | How the gateway reaches a unit; `{node}` becomes the Slurm node's name. |
| `clusterAddress` | Where Slurm nodes reach this cluster: see [below](#reaching-the-cluster-from-a-slurm-node). |

The hostPath needs a release namespace that admits it: the `baseline` and `restricted` Pod Security
Standards do not.

### The job JSON

The job JSON decides everything about the job, including what runs: its `script` must start the
computing-unit master. Users only choose a name. For example, on a cluster where the computing-unit
image's contents are unpacked at `/opt/texera`:

```yaml
workflowComputingUnitManager:
  slurm:
    enabled: true
    nodeSelector:
      kubernetes.io/hostname: slurm-login-1
    jobJson: |
      {"job": {
        "partition": "texera",
        "shared": ["none"],
        "cpus_per_task": 8,
        "memory_per_node": {"set": true, "number": 32000},
        "time_limit": {"set": true, "number": 720},
        "current_working_directory": "/tmp",
        "environment": [
          "PATH=/usr/bin:/bin",
          "JAVA_OPTS=-Xmx16g",
          "STORAGE_JDBC_URL=jdbc:postgresql://db.hpc.example.edu:5432/texera_db?currentSchema=texera_db,public"
        ],
        "script": "#!/bin/bash\nmkdir -p \"/tmp/cu-$TEXERA_CU_ID\" && cd \"/tmp/cu-$TEXERA_CU_ID\"\nexec /opt/texera/amber/bin/computing-unit-master\n"
      }}
```

- **Environment.** The manager adds to `job.environment` what a unit needs, as it does for a pod:
  storage endpoints and credentials, `TEXERA_CU_ID`, and the owner's `USER_JWT_TOKEN`. Its addresses
  are cluster-internal unless `clusterAddress` is set. Your entries win, except for `TEXERA_CU_ID`
  and `USER_JWT_TOKEN`.
- **One unit per node.** A unit binds fixed ports (8085 and 2552), so the job JSON must keep units on
  separate nodes: `"shared": ["none"]` as above (`"exclusive": ["true"]` before `v0.0.42`), or a
  dedicated GRES. A job Slurm places on a node already hosting a unit is refused an address, so its
  user is never routed to the other unit, but it will fail when it cannot bind those ports.
- **Network.** The gateway must reach each node on 8085, resolving the name in
  `unitAddressTemplate`.
- **Workspace.** The working directory must be writable, and unique per unit if nodes share it.
- **Time limit.** The idle sweep (`KUBERNETES_COMPUTING_UNIT_IDLE_CLEANUP_ENABLED`) only covers
  Kubernetes units, so `time_limit` is what bounds a forgotten unit.

### Reaching the cluster from a Slurm node

A unit on a Slurm node needs PostgreSQL, Lakekeeper, the object store, LakeFS and the file service,
none of which is reachable outside the cluster by default. Set `slurm.clusterAddress` to the address
Slurm nodes can reach this cluster's `LoadBalancer` services at, and the chart:

- Publishes each as a `LoadBalancer` service named `<release>-slurm-*`.
- Adds their addresses to the front of `job.environment`, so entries of your own still win.
- Moves the deployment's object-store endpoint to that address. Lakekeeper tells whoever reads a
  table which endpoint to use, so a Slurm node must be able to use the same one as everyone else,
  and pods must be able to reach the address too.

These services hold credentials that default to values published in `values.yaml`, so restrict the
address to the Slurm nodes and the cluster.

Set `LAKEFS_BLOCKSTORE_S3_PRE_SIGNED_ENDPOINT` as well, in `lakefs.extraEnvVars`. Presigned dataset
URLs otherwise name `localhost`, which only works where the object store's `NodePort` is forwarded.

If you can reach these services another way, leave `clusterAddress` empty and give their addresses
in `job.environment`, and set `storage.s3.endpoint` to an address that works everywhere.

### Isolation

A Slurm unit does not have the isolation of a pod. Before offering this to mutually untrusting users,
consider that:

- With `rest_auth/local`, every unit runs as one Slurm user. A user's UDF can act on other units' jobs
  (`scancel`), read their files on shared storage, and, on a node they share, read their environment.
  With tokens each unit runs as its own user, which separates units from each other, but see below.
- That environment holds the database and object-store credentials, the owner's token, and
  `AUTH_JWT_SECRET`, the key that signs every user's Texera token. Anyone who can read it can forge a
  token for any user, including an administrator. With tokens, that means every user who creates a unit,
  through their own Slurm account, without needing a UDF. It is also stored in slurmctld's
  `StateSaveLocation` while the job exists, and in the accounting database if `AccountingStoreFlags`
  includes `job_env`.
- A unit trusts the identity headers the gateway sets, and a pod is only reachable from inside the
  cluster. Port 8085 on a Slurm node must likewise be firewalled so that only the gateway reaches it,
  or anyone who can reach it can act as any user.
