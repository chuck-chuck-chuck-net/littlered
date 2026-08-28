# In-Place Auth Enablement and Password Rotation — Design & Implementation Brief

**Status: DESIGN ONLY. No code was written.** Prospective **ADR-019** — verified: `docs/adr/`
currently holds 001…009, 011, 013…018, so **018 is the highest and 019 is free**. (017 was
*State-Gated Intra-Shard Rolling Updates*, taken while the rename design still had it reserved;
the rename shipped as 018. Re-check with `ls docs/adr/` before writing the file.)

**Changelog ID:** unallocated. Allocate with the LR-039 cross-branch loop over **every** branch,
not by reading the tip of one line. The highest ID visible on `e2e-0821` is **LR-050**.

**Scope:** all four modes (`standalone`, `sentinel`, `failover`, `cluster`).
**Companion, already shipped:** the in-place Sentinel master-name rename (ADR-018 / LR-048 /
LR-050). This document is its §13 turned into a design. **Do the two operations in separate
windows** — that is N7 there and it stays true here.

---

## 0. ⚠ Read this first — three premises in the brief and in §13 are FALSIFIED

All three were checked against the Redis 8.4.2 and Valkey 8.1 sources **and** confirmed in a
local lab (podman, `redis:8.4.2`, this session, no cluster touched). Details and the exact
commands are in §2.

1. **§13: *"the operator's own probes carry the new password immediately, so an un-rolled pod
   answers `ERR Client sent AUTH, but no password is set` and reads as unreachable in the
   gather"* — FALSE for the enablement direction.** go-redis v9 sends
   `HELLO <ver> AUTH default <password>` — it substitutes the username `default` when
   `Options.Username` is empty (`go-redis/v9@v9.22.0/commands.go:363-370`), and the operator
   never sets `Username` (grep: zero hits in `internal/`, `cmd/`). The **three-argument** AUTH
   form hits ACL's `nopass` short-circuit (`redis/src/acl.c:1453`, `valkey/src/acl.c:1466`) and
   **succeeds against a password-less server**. Measured:
   `nopass server, client pass=P → OK`. So enabling auth does **not** blind the operator's
   gather. It is *rotation* that blinds it (`WRONGPASS`), and that distinction reshapes the
   whole design.
2. **§13: *"a multi-password window is fragile under any restart unless the operator
   re-establishes it, which argues for driving it from the ConfigMap/args (i.e. a rollout)
   rather than from a live `ACL SETUSER`"* — the conclusion is right, the premise is
   incomplete.** A multi-password default user **is expressible in the pod's argv**
   (`--user default on '>old' '>new' '~*' '&*' +@all`) and therefore survives every restart. It
   also **silently overrides `--requirepass`** regardless of argv order, because
   `initServer()` applies `requirepass` (`redis/src/server.c:3021`) and
   `ACLLoadUsersAtStartup()` runs later (`:7749`) and starts with `reset`
   (`redis/src/acl.c:2232`). Measured: a server started with **both** accepts `oldpw`/`newpw`
   and rejects `argpw`, while `CONFIG GET requirepass` still reports `argpw`. That is a
   diagnostic trap and a design opportunity in one.
3. **The brief's Q4 premise — *"the operator talking to a mixed-credential fleet … six call
   sites, one password per pass, no 'try both'"* — is a prerequisite only for designs this
   document rejects.** With the staged design below the operator's single password is valid on
   **every** pod at **every** instant, in both features. **No "try both" is needed and the six
   call sites do not change.** See §5, Q4.

Nothing else in §13 is wrong. Its two named pitfalls are both confirmed; its "overlap on the
client edge, atomic switch on the internal edges" instinct is **half right** — the internal
edges turn out to have an overlap too, just a different one.

---

## 1. What this is

Two operations the product does not implement today and which its own runbooks ask for
(`docs/USAGE.md` "Isolating Sentinel instances" step 2: *"Enable authentication"*; pillar 3.7:
*"authentication is strongly recommended"*; ADR-015: *"the concrete reason auth is recommended
rather than merely nice"*).

> **Enablement** — flip `spec.auth.enabled` from `false` to `true` on a running instance, in
> place, without losing data and without the operator, Sentinel, or replication ever seeing a
> peer it cannot talk to.
>
> **Rotation** — replace the password of an already-authenticated instance, in place, with a
> window in which **both** passwords are accepted, so clients migrate one at a time.

They are **two features with different achievable guarantees** and they must be designed
separately (Q1). They share one substrate (Q4).

### 1.1 Requirements

| # | Requirement |
|---|---|
| R1 | Enabling auth on a **healthy** instance of any mode converges with no operator-side human action, and **no pod pair is ever mutually unauthenticable** — not operator↔pod, not replica↔master, not Sentinel↔Sentinel, not Sentinel↔master. |
| R2 | **The dataset survives**, in every mode. No path where a credential mismatch is read as "no data holders" (that is the LR-015 wipe, §6.3). |
| R3 | Rotation offers a **client-visible overlap**: an interval in which both the old and the new password authenticate, so clients migrate individually. |
| R4 | **Resumable, no persisted phase.** The operator may die at any point; the next pass re-derives everything from the CR, the Secret and the live StatefulSets (ADR-006, LR-047's "the live object's own field is the cursor"). |
| R5 | **Observable**: a condition + events + log lines naming the current stage and, when it defers, why. |
| R6 | **Safe to do nothing.** If a stage's preconditions do not hold, the operator holds and says so; a held auth change is an unfinished change, never an outage (LR-047's asymmetry). |
| R7 | Robust on the **happy path**: platform stable, no node loss, no concurrent disruption, quorum intact. |
| R8 | The password never appears in a Kubernetes object the operator writes. (Today's invariant — §4 — must not regress.) |

### 1.2 Explicit non-requirements — and why we can afford them

| # | Non-requirement | Why we can afford it |
|---|---|---|
| N1 | **A maintenance window.** Neither feature requires one. | This is a *result*, not a concession — see Q2. It is listed as a non-requirement because the rename design's window was a *precondition*, and a reader coming from that document will expect one here. Do not add one "to be safe": a window does not make a single-stage change safe, it only hides the client half of the damage (§6.3). |
| N2 | **Zero pod restarts.** All pods roll, once per stage. | The credential is read only at process start (§4); there is no live path that survives a restart (§2 F2/F3). Under EmptyDir a serialized rolling restart is a resync, not a loss — the same reasoning as the rename's N3. |
| N3 | **A single-step enablement.** Enabling auth is **two** rollouts, and rotation is **three**. | Each extra stage removes an entire class of mixed-credential failure (§6). One stage buys ~3 minutes per mode; the class it removes includes a measured-elsewhere data-loss shape (LR-015). This is the central trade of the document and it is not close. |
| N4 | **Rotation for clients that send bare two-argument `AUTH`.** They are covered. **Enablement** for such clients is a cutover. | Rotation's overlap is server-side (two accepted passwords), so *any* client form works. Enablement's overlap is the `nopass` short-circuit, which only the **username** form reaches (§2 F1). A client that sends `AUTH <pw>` with no username gets an error from a not-yet-rolled pod. Stated plainly in the runbook; it is a client-configuration precondition, not something the operator can fix. |
| N5 | **Changing `spec.auth.existingSecret` to a different Secret as the rotation mechanism.** | It works mechanically (the `secretKeyRef.Name` is in the pod template, so it rolls — §4) but it gives no overlap: one Secret, one password. Rotation is expressed as **two keys in one Secret** instead (§5, Q3). Swapping the Secret *name* remains supported and is treated exactly like a rotation whose stages the user must run by hand. |
| N6 | **Disabling auth (`true → false`).** Designed for, specified, but not a shipping requirement of the first milestone. | It is the exact mirror of enablement (§5.4) and falls out of the same renderer. Deferring it costs nothing and keeps the first milestone's blast radius down. |
| N7 | **Protecting the cluster bus.** | It has **no** password authentication at any supported version — re-confirmed here, zero hits for `requirepass`/`masterauth`/`primaryauth` in `redis/src/cluster_legacy.c` (8.4.2) and `valkey/src/cluster_legacy.c` (8.1). Pillar 3.4 and LR-043 stand unchanged; `spec.auth` is a **client-edge** control in cluster mode and `docs/USAGE.md` already says so. Do not let this work be described as "securing cluster mode". |
| N8 | **Per-user ACLs, `masteruser`/`sentinel-user` as a product feature, or an `aclfile`.** | The username `default` is used as a *mechanism* in exactly one transient stage (§5.3) and nowhere else. `aclfile` is actively forbidden: it is mutually exclusive with `user` directives and the server **exits** if both are present (`redis/src/acl.c:2570-2577`). |
| N9 | **Doing the rename (ADR-018) in the same window.** | One variable per window. Both operations roll the same StatefulSets and both interact with the LR-050 attribution gate. |

---

## 2. PHASE 1 — Redis/Valkey semantics, source-confirmed and lab-confirmed

Every row was read in **both** `redis/redis@8.4.2` and `valkey-io/valkey@8.1` (LR-024's
precedent), and the ones marked **LAB** were additionally executed against `redis:8.4.2` in a
local podman container in this session. Nothing here required a cluster.

### 2.1 The table

| # | Question | Answer | Evidence |
|---|---|---|---|
| **F1** | Is `nopass` **and** a password a reachable state? | **No.** `nopass` empties the password list; `>pw` and `#hash` clear `nopass`. The two are mutually exclusive by construction. | redis `acl.c:1298-1300` (`nopass` → `listEmpty(u->passwords)`), `:1322` (`>pw` → clears `USER_FLAG_NOPASS`); valkey `acl.c:1311-1312`, `:1335`. **Consequence: a server can never simultaneously accept "the password" and "no credentials". There is no server-side overlap for enablement.** |
| **F1b** | Does `AUTH` accept anything against a `nopass` server? | **Two-argument `AUTH <pw>`: NO** — explicit error. **Three-argument `AUTH default <anything>`: YES** — `ACLCheckUserCredentials` returns `C_OK` on the `nopass` flag before looking at the password at all. | redis `acl.c:3254-3262` (2-arg refusal), `:1453` (nopass short-circuit), `:1490-1516` (`checkPasswordBasedAuth` → `ACLAuthenticateUser`); valkey `acl.c:3198-3206`, `:1466`. `HELLO <n> AUTH <u> <p>` takes the identical path (redis `networking.c:4247-4257`). **LAB:** `AUTH secret` → `ERR AUTH <password> called without any password configured for the default user…`; `AUTH default secret` → `OK`. |
| **F1c** | Exact error text? | `ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?` — **not** the pre-6.0 `ERR Client sent AUTH, but no password is set` that §13 quotes. | redis `acl.c:3255-3258`; valkey `acl.c:3199`. **LAB.** Do not write an error-string matcher against the old text. |
| **F1d** | What does **go-redis** send? | `HELLO <proto> AUTH default <password>` whenever a password is set and `Username` is empty. It always tries `HELLO` first and only falls back to legacy `AUTH` if `HELLO` errors. | `go-redis/v9@v9.22.0/commands.go:363-370`, `redis.go:793-819`. The operator sets no `Username` anywhere. **LAB (a Go program built against the repo's own go-redis version):** `nopass server, client pass=P → OK`; `requirepass=X, pass="" → NOAUTH`; `requirepass=X, pass=WRONG → WRONGPASS`; `2-pw server (a,b), pass=a → OK`, `pass=b → OK`. |
| **F1e** | What does **redis-cli** send? | `AUTH <pw>` (2-arg) unless `--user` is given, in which case `AUTH <user> <pw>`. On AUTH failure it prints `AUTH failed: …` to stderr but **continues** and, against a `nopass` server, the command still succeeds with **exit code 0**. | `redis-cli.c:1587-1608`, `:1729-1730`. **LAB:** `redis-cli -a secret PING` against nopass → `AUTH failed: …` then `PONG`, exit 0; `redis-cli --user default -a secret PING` → clean `PONG`. Relevant because every startup script, preStop hook and probe in this repo uses `redis-cli -a` (§4). |
| **F2** | What does `CONFIG SET requirepass` do to the password list? | **Collapses it to exactly one** and clears `nopass`: `updateRequirePass` → `ACLUpdateDefaultUserPassword` → `resetpass` (clears the flag *and* empties the list) then a single `>pw`. | redis `config.c:2591-2596`, `acl.c:3288-3299`; valkey `config.c:2623-2628`, `acl.c:3224-3235`. **LAB:** a server holding `{oldpw,newpw}` given `CONFIG SET requirepass finalpw` afterwards accepts only `finalpw`; `ACL GETUSER default` shows one hash. **§13's pitfall (1) confirmed.** |
| **F3** | `ACL SETUSER default >old >new` — does `AUTH` accept either? Does it survive `CONFIG REWRITE`? A restart? | **Either: yes** (the loop at redis `acl.c:1456-1470` tries every stored hash). **`CONFIG REWRITE`: yes** — the whole user is rewritten as a `user default …` line when no `aclfile` is set (redis `config.c:1404-1432`). **A restart: only if the `user` line is in the pod's config/argv** — an `ACL SETUSER` issued at runtime is lost, because our pods re-copy the ConfigMap over `/data/*.conf` on every start (§4). | **LAB:** both passwords accepted in both AUTH forms. |
| **F3b** | Is a multi-password default user expressible in **argv**, and how does it interact with `--requirepass`? | **Yes**, and **the `user` directive wins**, regardless of argv order. `initServer()` applies `requirepass` first; `ACLLoadUsersAtStartup()` runs later and, for `default`, issues `reset` before applying the rules. `server.requirepass` keeps the stale value, so `CONFIG GET requirepass` **lies**. | redis `server.c:3021` vs `:7749`, `acl.c:2225-2237`; valkey `server.c:2981` vs `:7171`, `acl.c:2229-2232`. **LAB:** `redis-server --requirepass argpw --user default on '>oldpw' '>newpw' '~*' '&*' +@all` → `oldpw` OK, `newpw` OK, `argpw` **WRONGPASS**, `CONFIG GET requirepass` → `argpw`. No warning is logged. |
| **F3c** | ⚠ **A `user` line in the config file *and* in argv is FATAL.** | `ACLAppendUserForLoading` refuses a duplicate username: *"Duplicate user found. A user can only be defined once in config files"* → the server **exits at startup**. | redis `acl.c:2178-2182`. **LAB:** a Sentinel that had `CONFIG REWRITE`-ten its own `user default …` line, restarted against that file **plus** the same argv, died with `*** FATAL CONFIG FILE ERROR ***`. **This is only survivable today because every startup script unconditionally `cp`s the ConfigMap over the writable copy** (§4). See risk **K5**. |
| **F4** | `masterauth` — single-valued? live-settable? re-auth on next handshake or only on reconnect? | **Single-valued** (`createSDSConfig`). **Live-settable** (`MODIFIABLE_CONFIG`), with **no** apply function — so `CONFIG SET masterauth` changes nothing about an established link. It is read at **handshake** time only (`REPL_STATE_SEND_HANDSHAKE`), i.e. it takes effect on the **next connection**, never on the current one. An established replication link therefore survives a password change on the master until something drops it. | redis `config.c:3155`, `replication.c:2971-2989` and `:3565-3590` (the rdb-channel handshake), `:3025`; valkey `config.c:3253` (`primaryauth`, with `masterauth` retained as an alias), `replication.c:2699-2712`. |
| **F4b** | Does `masteruser default` make the replication edge tolerant of a `nopass` master? | **Yes**, and it does **not** weaken refusal of a wrong password against a *protected* master. | redis `replication.c:2973-2984` sends the 3-arg form iff `server.masteruser` is set. **LAB, four containers:** replica `--masteruser default --masterauth P1` → nopass master: `master_link_status:up`, *"MASTER <-> REPLICA sync: Finished with success"*. Replica `--masterauth P1` (no masteruser) → same nopass master: *"Unable to AUTH to MASTER: -ERR AUTH <password> called without any password configured…"*, link down. Replica `--masteruser default --masterauth P1` → master with `requirepass P2`: `WRONGPASS`, link down. Replica `--masteruser default --masterauth P2` → same master: link up. |
| **F5** | Sentinel's `sentinel-pass` — single-valued? live-settable? what happens while three Sentinels disagree? | **Single-valued.** **Live-settable** via `SENTINEL CONFIG SET sentinel-pass` (whitelisted), which also **drops all Sentinel connections** to force a reconnect, and is persisted by `sentinelFlushConfig`. **A disagreement is one-directional, not symmetric:** `sentinelSendAuthIfNeeded` sends AUTH and **discards the reply** (*"We don't check at all if the command was successfully transmitted"*), so a Sentinel presenting a credential to a peer that demands none still works; a Sentinel presenting **nothing** to a peer that demands a password gets `NOAUTH` on every subsequent command and the link is treated as down. So a rolled→un-rolled probe succeeds and an un-rolled→rolled probe fails. Losing peer links loses `is-master-down-by-addr` votes, i.e. the failover **election**. | redis `sentinel.c:254`, `:1980-1983`, `:2295-2344`, `:3183-3196` and `:3282-3296` (`drop_conns` → `sentinelDropConnections()`), `:2230-2235` (rewrite); valkey `sentinel.c:273`, `:1941-1943`, `:2263-2266`, `:3073`, `:3165-3168`, `:2173-2179`. **LAB:** `SENTINEL CONFIG SET sentinel-pass NEWSP` → `OK`, `CONFIG GET` reflects it, and the value lands in the rewritten conf. |
| **F5b** | Can a Sentinel's **incoming** requirement (`requirepass`) be changed live? | **No.** `CONFIG` is not a Sentinel command at all. The `SENTINEL CONFIG SET` whitelist is `announce-ip`, `sentinel-user`, `sentinel-pass`, `resolve-hostnames`, `announce-port`, `announce-hostnames`, `loglevel` — `requirepass` is absent. | redis `sentinel.c:3186-3196`. **LAB:** `CONFIG SET requirepass NEWSP` on a Sentinel → `ERR unknown command 'CONFIG'`. **Changing what a Sentinel demands requires a restart, i.e. a rollout. This is the constraint that makes the staged design mandatory rather than merely tidy in sentinel mode.** |
| **F5c** | Does a **Sentinel** honour the `user` directive (multi-password on its own listener)? | **Yes.** | **LAB:** `redis-sentinel … --user default on '>a' '>b' '~*' '&*' +@all` starts, accepts both `a` and `b`, and answers `NOAUTH` to an unauthenticated client. **But** `sentinelSendAuthIfNeeded` falls back to `server.requirepass` when `sentinel-pass` is unset (redis `sentinel.c:2322-2327`), which the `user` form leaves empty — so whenever the `user` form is used on a Sentinel, `sentinel-pass` **must** be set explicitly. The current builder already sets it (§4). |
| **F6** | Sentinel's per-master `auth-pass` — live-settable? survives Sentinel's own config rewrite? | **Both yes.** `SENTINEL SET <master> auth-pass <pw>` (and `auth-user`) sets it, calls `dropInstanceConnections(ri)`, and `sentinelFlushConfigAndReply` persists it. | redis `sentinel.c:4418-4433`, `:4496`, `:2103-2116`; valkey `sentinel.c:1860-1866`, `:2053-2064`, `:3165`. **LAB:** `SENTINEL SET mm auth-user default` + `auth-pass P1` against a **nopass** master → the Sentinel reads the master's `INFO` fine (`runid` populated, `flags: master`, no `s_down`), and both lines appear in the rewritten `sentinel.conf`. |
| **F7** | The cluster bus. | **Unauthenticated at every supported version.** `grep -c 'requirepass\|masterauth\|primaryauth'` in `redis/src/cluster_legacy.c` (8.4.2) → **0**; `valkey/src/cluster_legacy.c` (8.1) → **0**. The `CLUSTERMSG_TYPE_FAILOVER_AUTH_*` messages are election votes, not authentication. `spec.auth` protects the **client port only**. | Confirms pillar 3.4, LR-043 and `docs/USAGE.md`'s cluster paragraph verbatim. |
| **F7b** | Native atomic slot migration (`CLUSTER MIGRATION IMPORT`, Redis 8.4+) — which credential? | **Neither.** It authenticates with `AUTH "internal connection" <secret>`, a cluster-internal secret, not `requirepass`/`masterauth`. So the ASM reshard path is **credential-independent** and unaffected by this work. | redis `cluster_asm.c:1284-1292`, `:1327-1347`, `:1477-1497`; the user is handled by `internalAuth` (`acl.c:3273-3277`). Not present in Valkey 8.1 (no `cluster_asm.c`) — the pre-8.4 dance is the only path there. |
| **F7c** | The pre-8.4 dance (`MIGRATE`) — which credential? | `MIGRATE … AUTH <password>`, the **two-argument** form. Against a `nopass` destination that errors (F1b), so a reshard in flight across a mixed fleet fails. `MIGRATE … AUTH2 <user> <pw>` is the tolerant form and is what `redis-cli --cluster` uses when a user is configured. | `internal/redis/cluster_migrate.go:149-152`; redis `cluster.c:414-420`, `:447-462`, `redis-cli.c:8074-8079`. See work package **WP6**. |

### 2.2 Where the projects differ

Only in naming and file layout, never in behaviour:

- Valkey renames `masterauth`/`masteruser` to `primaryauth`/`primaryuser` and **keeps the old
  names as aliases** (`valkey/src/config.c:3227`, `:3253`), so the argv this operator emits
  works unchanged on both.
- Valkey has no `cluster_asm.c`, hence no native atomic slot migration; the operator's free
  capability probe (LR-018) already selects the dance there, so F7b is a Redis-8.4-only concern.
- Every ACL, replication-handshake and Sentinel-auth code path cited above is line-for-line
  equivalent. **No behavioural divergence was found.**

### 2.3 The three facts the design rests on

1. **F1 + F1b.** A server cannot accept both "P" and "nothing", but a *client that names the
   user* is accepted by a `nopass` server. So enablement's overlap does not live on the server —
   it lives in the **form of the credential the peer presents**. That is why enablement is split
   into a *present-only* stage and an *enforce* stage, and why the present-only stage uses the
   username form.
2. **F3b.** A server **can** accept two real passwords, from argv, across restarts. So
   rotation's overlap *does* live on the server, and needs no client cooperation at all.
3. **F5b.** A Sentinel's incoming requirement cannot be changed without a restart, and F4/F5/F6
   say every *outgoing* credential can. So the only edge that is restart-bound is the one that
   *demands*, and staging the demand after the presentation is exactly what makes every
   intermediate state safe.

---

## 3. PHASE 2 — Ground truth in this repo

Verified against the working tree on `e2e-0821` at `9f8b910`. **Re-verify the line numbers, not
the facts** — they have drifted before.

### 3.1 The API surface

- `api/v1alpha1/littlered_types.go:209-219` — `AuthSpec` has exactly two fields, `Enabled`
  (`+kubebuilder:default=false`) and `ExistingSecret`. **No immutability marker, no CEL**;
  `config/crd/bases/redis.chuck-chuck-chuck.net_littlereds.yaml:64-75` carries no
  `x-kubernetes-validations`. `spec.auth` is freely mutable today and nothing refuses an
  in-place flip.
- Embedded at `:52-54` as a **value, not a pointer**, so there is no unset-vs-zero distinction.
- The Secret key is **hardcoded**: `internal/controller/resources.go:155`
  `secretKeyPassword = "password"`. There is no `passwordKey` field.
- **`ConditionAuthReady = "AuthReady"` is declared at `api/v1alpha1/littlered_types.go:654-655`
  and never used anywhere.** It is free for this feature (§5.6).
- The only validation is imperative, in `validateSpec`
  (`internal/controller/littlered_controller.go:296-316`): enabled ⇒ `existingSecret` non-empty,
  the Secret must exist and must carry a `password` key. It re-runs every pass, so it does react
  to a spec edit — but it validates *existence*, never that the pods agree with it.
- **Side effect worth knowing:** `quarantineConfigDangerous`
  (`internal/controller/littlered_controller.go:2287-2290`) reads `!lr.Spec.Auth.Enabled &&
  SentinelMasterName() == LegacySentinelMasterName`. **Enabling auth raises the LR-044
  quarantine attempt budget from N=1 to N=2.** Correct, and it should be stated in the runbook
  rather than discovered.

### 3.2 How the password reaches a process, per mode

**The governing invariant, and it is the single most important mechanical fact here:** the
password never enters a ConfigMap and never appears literally in any object the operator writes.
Every mode delivers it as the `REDIS_PASSWORD` env var from a `secretKeyRef` and expands it at
the last moment — either K8s `$(REDIS_PASSWORD)` argv expansion or shell `$REDIS_PASSWORD`
inside the startup script. `buildRedisConfig` says so at `resources.go:330-331`;
`buildSentinelConfig` repeats it at `:896-906`.

**Therefore the pod template is byte-identical before and after a password change.**

| mode | present/enforce sites | file:line |
|---|---|---|
| standalone | env `secretKeyRef`; `--requirepass $(REDIS_PASSWORD)` as an **argv element**; liveness and readiness probes carry `-a $(REDIS_PASSWORD) --no-auth-warning` | `resources.go:472-483`, `:485`, `:624-625`, `:655-656` |
| sentinel — data pods | shell-built `AUTH_ARGS="--requirepass $REDIS_PASSWORD --masterauth $REDIS_PASSWORD"` and `SENTINEL_AUTH_ARGS="-a $REDIS_PASSWORD --no-auth-warning"`, appended to all three `exec redis-server` branches and used by the kill-9 guard, the wait-loop poll and the master PING | `resources.go:1134-1138`; execs `:1191`, `:1213`, `:1221`; probes `:1148`, `:1150`, `:1170`, `:1204` |
| sentinel — data pods, preStop | `AUTH_ARGS="-a $REDIS_PASSWORD --no-auth-warning"` for `INFO`, `SENTINEL SLAVES`, `CLIENT PAUSE`, `SENTINEL failover` | `resources.go:1256-1291`, wired `:1370-1376` |
| sentinel — sentinel pods | `AUTH_ARGS="--requirepass $REDIS_PASSWORD --sentinel sentinel-pass $REDIS_PASSWORD"`, appended to `exec redis-sentinel`. **The only `sentinel-pass` site** — pillar 3.7's peer-membership credential. `auth-pass` is **not** here; the operator pushes it at runtime (§3.4). | `resources.go:1553-1555`, `:1559` |
| failover | `AUTH_ARGS="--requirepass $REDIS_PASSWORD --masterauth $REDIS_PASSWORD"` on both exec branches; preStop uses `-a`. The startup **start-gate reads the downward-API annotations file and needs no credential** — the only startup path in the product that authenticates to nothing. | `resources_failover.go:334-336`, `:422`, `:429`, `:596-598`; gate `:329-331` |
| cluster | `--requirepass "${REDIS_PASSWORD}" --masterauth "${REDIS_PASSWORD}"` emitted **unconditionally** (not inside an `if [ -n … ]`), so with auth off the server gets `--requirepass ""` which `EMPTY_STRING_IS_NULL` maps to nopass. STEP-3 yield probes and preStop build their own `-a` args. | `resources.go:2189-2196`, `:2124`, `:2259-2261` |
| exporter sidecar, **all four modes** | `REDIS_PASSWORD` from the same `secretKeyRef`; `redis_exporter` reads the env itself | `resources.go:505-516`; attached at `:370`, `:1058`, `:1492`, `:1997`, `resources_failover.go:242` |

Two shapes exist for the probes and they behave differently under a change:

- `-a $(REDIS_PASSWORD)` — emitted **only when `Spec.Auth.Enabled`** (`resources.go:624-625`,
  `:702-703`, `:2490-2491`, `:2520-2521`). Not tolerant: the flag's presence is a template
  decision.
- `AUTH=""; [ -n "$REDIS_PASSWORD" ] && AUTH="-a $REDIS_PASSWORD --no-auth-warning"` — emitted
  **unconditionally** and self-disabling (`resources.go:736`, `:1596`, `:1610`). Tolerant.

The second shape is what this feature wants everywhere; the asymmetry is pre-existing and
harmless today only because both halves change together.

**No initContainers exist in any mode.** All bootstrap logic is in the main container's
`Command`.

**Every startup script `cp`s the read-only ConfigMap over the writable copy on every start** —
`resources.go:1125` (sentinel data), `:2116` (cluster), `:1550` (sentinel processes, `cp
/etc/sentinel/sentinel.conf /data/sentinel.conf`), `resources_failover.go:319`. Nothing in the
operator ever issues `CONFIG REWRITE` against a redis-server. **This is what makes F3c
survivable** and it is now load-bearing (risk K5).

### 3.3 `getRedisPassword`

`internal/controller/littlered_controller.go:1805-1818`:

```go
func (r *LittleRedReconciler) getRedisPassword(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) string {
	if !littleRed.Spec.Auth.Enabled {
		return ""
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{...}, secret); err != nil {
		return ""
	}
	return string(secret.Data["password"])
}
```

Four properties matter:

1. **It returns one value and no error.** Auth off → `""`. Secret missing, RBAC-denied, cache
   cold, any error → **also `""`, silently**. A misread Secret is indistinguishable from "auth is
   off", and the operator will dial an enforcing fleet with an empty password and read the whole
   instance as unreachable. **This is a pre-existing defect and it is on this feature's critical
   path** (WP1).
2. **It reads through the cached client** (`r.Get`; RBAC `get;list;watch` on `secrets` at
   `littlered_controller.go:143` and `config/rbac/role.yaml:34`). The uncached `r.apiReader()`
   is never used for Secrets. So **the operator sees a Secret edit at informer speed and the
   pods see it only when they restart** — that skew *is* the rotation problem.
3. **It is not cached by the reconciler** — re-called per pass, per subsystem.
4. **Eleven call sites**, all in `internal/controller`: `:842` (the whole sentinel healing pass →
   the gatherer at `:931`, every `NewSentinelClient` at `:1042`/`:1129`/`:1149`, the
   `SENTINEL SET auth-pass` pushes at `:1048`/`:1134`/`:1154`, Rule R's `SlaveOf` at `:1265`),
   `:1336` (`getMasterPodName`), `:1586` (status), `:1882` (`bootstrapSentinel` →
   `seedSentinelsWithMaster` → `auth-pass` at `:1977`), `cluster_reconcile.go:169`, `:688`,
   `:727`, `cluster_migration.go:398`, `:552`, `failover_reconcile.go:313`.

**Two paths deliberately bypass it** and re-read the Secret once per goroutine start, continuing
with `""` on error: `monitorSentinel` (`sentinel_monitor.go:87-108`) and `monitorFailoverMaster`
(`failover_monitor.go:170-191`). The comment at `failover_monitor.go:170-172` — *"A secret
rotation is picked up when the watcher is restarted (same staleness contract as the sentinel
monitor)"* — **is the only acknowledgement of rotation anywhere in the tree.**

### 3.4 Where the operator authenticates to a pod

| path | client | bound? | on auth failure |
|---|---|---|---|
| sentinel/failover gather — Redis | `gatherer.go:33-56` → `GetReplicationInfo` (`client.go:572-599`) → `newBoundedRedisClient` (`:115-124`) | ctx 3s + Dial/Read/Write 3s | error → `Reachable:false` |
| sentinel gather — Sentinel | `gatherer.go:58-110` → `GetMasterState` (`client.go:178-208`) → `newBoundedClient` (`:97-106`) | same | only `"ERR No such master"` / `"redis: nil"` are mapped to reachable-but-bare (`gatherer.go:80`); **`NOAUTH`/`WRONGPASS` fall through to `Reachable:false`** |
| cluster gather | `gatherer.go:138-162` → `getBoundedClient` (`cluster_client.go:123-132`) | same | `Reachable:false` |
| cluster repair/migration | `NewClusterClient` (`cluster_reconcile.go:170`, `cluster_migration.go:399`) | control commands `boundedCtx` 3s; `MIGRATE`/pipelines keep the long budget | command error, logged |
| sentinel writes `Monitor`/`Set`/`Reset`/`Remove` | `newBoundedClient` | LR-040 | **errors discarded** (`_ = podSC.Set(...)` at `:1048`, `:1134`, `:1154`, `:1977`) |
| `SlaveOf` | `client.go:535-546` | LR-049 | error returned, logged |
| failover monitor | `failover_monitor.go:250-259` | 3s | **`return err == nil` — an auth failure counts as "master down"** and past `downAfterMilliseconds` fires a `GenericEvent` |
| sentinel `+switch-master` subscriber | `sentinel_monitor.go:119` → `Subscribe` | deliberately unbounded | logged, retried every 10s forever |
| **`lrctl`** | `cmd/lrctl/cmd/redis_exec.go:36-55` + `internal/cli/k8s/client.go:59` | n/a | **no password ever enters the lrctl process** — every exec carries `AUTH=""; [ -n "$REDIS_PASSWORD" ] && AUTH="-a $REDIS_PASSWORD --no-auth-warning";` and reads the *target container's own* env |

Two structural notes:

- **`lrctl` is inherently credential-skew-immune and the operator is not.** `lrctl` always holds
  *that pod's* password; the operator holds one password for the whole fleet. In any window
  where the pods disagree with the Secret, **`lrctl verify` is right and the operator is
  blind** — the reverse of LR-041's asymmetry, and it makes `lrctl` the ground-truth tool for
  this operation.
- **Nothing in the tree ever inspects a Redis error for `NOAUTH`, `WRONGPASS` or `AUTH`.** There
  is no auth-aware error classification at any layer. (Those strings appear only in a comment at
  `cmd/lrctl/cmd/redis_exec.go:28` and in e2e assertions.)

### 3.5 THE KEY TRACE — what a mixed-credential fleet looks like to the gather today

`internal/redis/gather.go:78-81`, verbatim:

```go
rs, err := g.GetRedisState(ctx, name, ip)
if err != nil {
    rs = &RedisNodeState{PodName: name, IP: ip, Reachable: false}
}
```

**Answer: the pod reads `Reachable:false`, and the error is discarded on that line.**
`RedisNodeState` (`internal/redis/replication_state.go:25-43`) has **no `Err` field** —
`PodName, IP, Role, MasterHost, LinkStatus, Offset, Keys, Replid, Replid2, Reachable`. The zero
value of every other field is exactly what a credential mismatch produces: `Role:""`, `Keys:0`,
`LinkStatus:""`, `Replid:""`. **A credential mismatch and a dial timeout are byte-identical to
every rule in the operator**, and there is no log line on that branch. The cluster path
(`gather.go:175-179`) and the sentinel path (`gatherer.go:80`, `gather.go:93-96`) have the same
shape.

But — and this is the falsified premise from §0 — **the enablement direction does not produce a
mismatch at all.** The operator holds P; a not-yet-rolled pod is `nopass`; go-redis sends
`HELLO … AUTH default P`; the pod says OK. It is the **rotation** direction (operator holds P′,
pod still has P) that yields `WRONGPASS`, and the **disable** direction (operator holds `""`,
pod still enforces P) that yields `NOAUTH`.

**Which rules then misfire, precisely** (this is the case for the whole design):

- **Rule L (LR-015) — the data-destroying one.** `DataHolders()` and `BestDataHolder()`
  (`replication_state.go:184`, `:212`) filter on `Reachable`, so every pod reads as **0 keys and
  not a data holder**. During a *rotation* the sentinel StatefulSet rolls fast (its probes are a
  bare PING) while the Redis StatefulSet rolls slowly behind `minReadySeconds: 35`. The
  intermediate state is: rolled Sentinels reachable (operator holds the new password) and
  **bare** (EmptyDir wiped their config), a reachable Sentinel quorum, **no reachable Redis
  master** (still on the old password), and **zero data holders**. That is precisely
  `planLeaderlessRecovery`'s no-data-reseed signature, it outlives the 30s cooldown by minutes,
  and the verdict is *"Reseeded"* — **the operator elects an empty master over a live, intact
  dataset.** No test at any tier covers this.
- **`planForsaken` / the quarantine (LR-042/LR-044).** `HasHealthyKnownReplica()` goes false;
  clause 1 needs a reachable *monitoring* Sentinel and clause 4 needs no reachable master of
  ours — an unreachable master satisfies clause 4 the wrong way. The quarantine's own
  `unverified` clause is **not** fooled, because LR-044's wiring half rekeyed it onto **kubelet
  readiness** (LR-023) rather than the operator's dial — a deliberate asymmetry already in the
  tree that this design can and does lean on.
- **Failover-mode death detection (`planMasterDeath`).** The probe-evidenced arm reads the
  master unreachable, but the corroboration clause needs replicas reporting `link:down` and
  unreachable replicas cannot corroborate, so it **HOLDs** — the LR-017 lesson doing its job.
  The K8s-authoritative arm is unaffected because the kubelet probe uses the *pod's own*
  credential. Net: the master looks dead to the operator and alive to the kubelet, and nothing
  is declared. **Safe, by an existing guard, for the right reason.**
- **Sentinel itself, independent of the operator.** An un-rolled Sentinel presenting no
  credential to a rolled, enforcing master gets `NOAUTH` on `INFO`, reaches `s_down`, and a
  quorum of un-rolled Sentinels reaches `o_down` and **fails over a perfectly healthy master**.
  This one is not an operator misfire at all; it is Sentinel behaving correctly on a topology we
  manufactured.
- **`SENTINEL SET auth-pass` failures are discarded** (§3.4). If the operator's password is
  stale, the Sentinels keep the old `auth-pass` and silently lose the ability to read their
  master, with no signal anywhere.

### 3.6 Rollout and change-detection machinery

- **`AnnotationPodSpecHash`** (`resources.go:43`, computed by `computePodTemplateHash` at
  `:186-200`) is stamped in **exactly one place**: `resources.go:2074`, inside
  `buildClusterShardStatefulSet`. **Cluster mode only.** It marshals the whole
  `PodTemplateSpec`, so it covers env var *names* and their `secretKeyRef` (name + key), args,
  scripts and probe commands — and **cannot** cover the Secret's *contents*.
- **`AnnotationConfigHash`** (`resources.go:42`, `:171-184`) hashes ConfigMap data only and is
  stamped in every mode (`:362`, `:1052`, `:1485`, `:1978`, `resources_failover.go:233`). Auth
  never enters a ConfigMap, so **it is completely insensitive to auth**.
- Consequently, today: `auth.enabled: false→true` changes the template in every mode (env entry
  and probe flags appear) and rolls. `existingSecret: a→b` changes `secretKeyRef.Name` and rolls.
  **A password rotated inside the same Secret changes nothing, hashes to nothing, and triggers
  no rollout in any mode.** That is the mechanical heart of the rotation problem.
- **Apply paths.** One helper, `apply` (`littlered_controller.go:1788-1803`) — server-side apply
  with `client.FieldOwner` **and `client.ForceOwnership`**. Standalone `:451`; sentinel data
  `reconcileRedisStatefulSetSentinel:758-762`; sentinel processes
  `reconcileSentinelStatefulSet:766-770`; failover `failover_reconcile.go:196-197`. **None of
  these is gated or serialized**, and the two sentinel StatefulSets roll **independently and
  concurrently**. Cluster is the exception: `reconcileClusterStatefulSet`
  (`cluster_reconcile.go:1123-1200`) rolls one shard at a time (LR-021) with ADR-017's
  intra-shard partition gate inside each.
- **`statefulSetRolloutSettled`** (`cluster_rollout.go:62-75`) — `ObservedGeneration ==
  Generation`, non-empty `UpdateRevision`, `UpdateRevision == CurrentRevision`, and
  `UpdatedReplicas == ReadyReplicas == Replicas == spec.Replicas`. **CLAUDE.md's
  `clusterShardRolloutSettled` name is stale**; it was renamed by LR-050.
- **LR-050's rollout attribution gate** — `rolling`, computed at
  `littlered_controller.go:890-928`, from an **uncached** read of the instance's own **Redis**
  StatefulSet (`statefulSetName(littleRed)`); `IsNotFound` → `false`, any other error → stays
  `true`. It suppresses exactly two things: `planForsaken` **arming** (`forsaken_plan.go:137`,
  `if rolling && !armed`) and Rule N's G5 `Foreign` verdict, downgraded to `Deferred`
  (`stale_master_name_plan.go:189`). It stops nothing else from running.

### 3.7 Existing tests

**Two unit tests, both shape checks:** `TestBuildStatefulSetWithAuth`
(`resources_test.go:455-490`, standalone env only) and `TestBuildLivenessProbeWithAuth`
(`:681-690`). That is the entire unit coverage.

`getRedisPassword` has **no test at all** — not the disabled path, not the missing-Secret path,
not the missing-key path.

**e2e:** `test/e2e/security_test.go:43-140` deploys each mode **with auth already on** and
asserts `NOAUTH` without the password and `PONG` with it. `test/e2e/auth_utils_test.go` is the
auth-on-by-default fixture infrastructure (sentinel and failover default auth-on; cluster and
standalone stay auth-free by design, `:53-58`). Auth is also used as an *experimental control*
in `sentinel_quarantine_test.go:622-626` and `sentinel_master_name_test.go:1239-1245` — a bogus
`masterauth` set live to stage a broken handshake.

**There is no test, at any tier, of enabling auth on an existing instance, of disabling it, or
of rotating a password.** `grep -rn "Spec.Auth" test/e2e/*.go` returns zero hits outside
creation-time literals. **The transition is entirely unexercised and, per §3.5, entirely
silent.** That is the red this feature is entitled to (§8).

---

## 4. Decision

> **Auth changes are driven from the pod template in *stages*, and a stage exists exactly where
> a peer would otherwise have to present a credential a peer does not yet accept. The operator
> renders the stages; the live StatefulSet's own applied template is the cursor; nothing is
> persisted.**

Concretely:

**Enablement is two stages.**

| stage | what every pod presents | what every pod demands |
|---|---|---|
| **E0** (today, auth off) | nothing | nothing |
| **E1 — Prepare** | `masteruser default` + `masterauth P` (redis), `sentinel-user default` + `sentinel-pass P` (sentinel), `auth-user default` + `auth-pass P` (operator-pushed), and the operator's own client (already the username form) | **nothing** |
| **E2 — Enforce** | the same, minus the `*-user default` flags | `requirepass P` |

E1 is inert by construction: every credential presented in E1 is accepted by a `nopass` peer
(F1b, F4b, F6), and nothing demands anything yet. E2 is inert by construction: by the time any
pod demands P, every pod already presents P. **At no instant is any pair of peers mutually
unauthenticable, and the operator is never blind.**

The `*-user default` flags are dropped at E2 **on purpose**: keeping them permanently would mean
a replica happily syncs from a *password-less foreign master*, which is exactly the
address-adoption path ADR-015 says `masterauth` closes. During E1 the instance is no less
protected than it is today (it has no auth at all); after E2 the posture is identical to a
freshly-created auth-on instance. **The username form is a transient mechanism, never a
posture.**

**Rotation is three stages, expressed by two keys in one Secret.**

| stage | Secret | what every pod presents | what every pod accepts |
|---|---|---|---|
| **R0** | `password: OLD` | OLD | OLD |
| **R1 — Widen** | `password: OLD`, `additionalPassword: NEW` | OLD | **OLD and NEW** |
| **R2 — Switch** | `password: NEW`, `additionalPassword: OLD` | NEW | **NEW and OLD** |
| **R3 — Narrow** | `password: NEW` | NEW | NEW |

Acceptance of both is rendered as `--user default on '>$REDIS_PASSWORD'
'>$REDIS_PASSWORD_ADDITIONAL' '~*' '&*' +@all` built in the startup script from the env, so the
literal never lands in a Kubernetes object (R8) and the state survives every restart (F3b).
Each stage's roll is inert for the same reason as above: the credential presented in each stage
is accepted by both the rolled and the un-rolled half. **Clients may migrate at any point
between R1 and R3, in any order, using any AUTH form** — that is R3's overlap, and it is
*stronger* than enablement's.

The operator **auto-inserts R2** (it can see both keys, so nothing is remembered), so the user
performs two Secret edits, not three. It cannot auto-insert R1 or R3, because it must not
remember a password the Secret no longer names.

**Why this and not the alternatives:** five properties, each of which is why one of §5's
alternatives lost.

1. **No mixed-credential state is ever reachable**, so §3.5's whole misfire catalogue — Rule L's
   empty reseed above all — is *designed out* rather than guarded against. Guards would have to
   be added to `planLeaderlessRecovery`, `planForsaken`, `quarantineDataRisk` and
   `planMasterDeath`, and LR-043's correction is explicit about what a growing pile of clauses on
   a safety predicate costs.
2. **The credential never changes at runtime**, so F2's collapse and F3's
   restart-fragility never bite. Everything is argv, everything survives a restart, and a pod
   that comes back mid-stage comes back correct.
3. **No persisted phase (R4).** The stage is a function of `(spec.auth, the Secret's keys, the
   live StatefulSets' applied template hash)`. LR-047 already established the pattern: *"the
   live StatefulSet's own field is the cursor"*.
4. **No cross-StatefulSet sequencing is needed.** Sentinel mode's two StatefulSets may roll
   concurrently and in any order, because every intermediate combination is inert. That deletes
   the hardest piece of wiring the naive design would have required.
5. **The operator's own six-plus call sites do not change** (§5, Q4), because its one password is
   valid on every pod at every instant.

---

## 5. Answering the six questions, with the recommendation for each

### Q1 — The two features' achievable guarantees

**Enablement.**

> *For every internal edge — operator↔pod, replica↔master, Sentinel↔Sentinel, Sentinel↔master —
> enablement is fully non-disruptive.* For **clients**, it is non-disruptive **iff** the client
> is configured with the password before stage E2 begins **and** authenticates with a username
> (`AUTH default <pw>`, `AUTH2`, or `HELLO n AUTH default <pw>`). A client that sends bare
> `AUTH <pw>` will get an error from any pod that has not yet reached E2; a client that sends no
> credential will get `NOAUTH` from any pod that has.

There is **no** server-side overlap available (F1: `nopass` and a password are mutually
exclusive), so the client half genuinely cannot be made unconditional. Say so plainly rather
than implying otherwise. Most Go, Python (`redis-py` ≥ 4 with `username=`), and Java clients can
send the username form; `redis-cli -a` without `--user` cannot.

**Rotation.**

> *Fully non-disruptive for everything, including every client and every AUTH form*, provided
> the user performs the R1 and R3 Secret edits with the R2 rollout settled in between.

The asymmetry is worth naming: **rotation, which sounds harder, has the stronger guarantee**,
because its overlap lives on the server (two accepted passwords) instead of in the client's
choice of AUTH form.

### Q2 — Window or no window, per feature — RECOMMENDATION

| | with a window | without a window | recommend |
|---|---|---|---|
| **Enablement** | Same two stages, same duration. The window buys **only** the client edge — it lets an owner reconfigure a bare-`AUTH` client between E1 and E2. | Two stages, ~1 Redis rollout each per mode. Clients must be pre-configured and username-capable. | **No window required.** Offer one as a *client-side* convenience in the runbook, explicitly not as a safety measure. |
| **Rotation** | Buys nothing at all. | Three stages (two user edits), clients migrate individually. | **No window, ever.** A window here would be a confession that the overlap does not work. |

**Do not offer a "quick" single-rollout variant of either.** It is the variant that reaches
§3.5's misfire catalogue, and one of those misfires (Rule L reseeding over a live dataset) is
data loss on a supported operation — the LR-050 shape exactly. The rename design could make a
window a *precondition* and get a simpler design for it; here a window buys no simplification,
so there is nothing to trade.

### Q3 — Live reconfiguration vs rollout vs a mounted file — RECOMMENDATION

Three candidate mechanisms:

**(a) Live reconfiguration** (`CONFIG SET requirepass`, `ACL SETUSER`, `CONFIG SET masterauth`,
`SENTINEL CONFIG SET sentinel-pass`, `SENTINEL SET auth-pass`). **Rejected as the mechanism.**
§13's objection is confirmed (F2, F3) and two more are added by measurement: a Sentinel's
**incoming** requirement cannot be set live at all (F5b — `CONFIG` is not a Sentinel command),
and every live setting is lost on the next container restart because the startup scripts re-copy
the ConfigMap (§3.2). A design that must re-establish state after every restart is one whose
correctness depends on the operator winning a race with the kubelet.

**(b) Drive it from args / the startup script.** **Recommended, and it is what the decision
does.** Everything needed is expressible: multi-password acceptance (F3b), the username forms
(F4b, F5c, F6), and the demand (`requirepass`). The startup script already builds `AUTH_ARGS`
from `$REDIS_PASSWORD` in shell, so extending it keeps the password out of every object (R8).
The cost is a rollout per stage, which is exactly the cost the staging already accepts.

**(c) §6.4's shape — read it from a mounted file.** **Applies here more cleanly than it did to
the master name, and is still deferred.** Redis config files support `include`, so the startup
script could write `/data/auth.conf` from the env and `include` it — but the *server* still only
reads it at start, so it buys no rollout avoidance whatsoever. It would only pay off with a
`CONFIG SET`-based re-read, i.e. mechanism (a), which is rejected. **Trigger to reopen:** if
Redis ever gains a reloadable ACL/`requirepass` source (an `aclfile` + `ACL LOAD` is the closest
thing that exists today), revisit — an `aclfile` would give a genuinely rollout-free multi-password
window. It is rejected *now* because it is mutually exclusive with `user` directives and the
server exits if both are configured (`acl.c:2570-2577`), which would make the two mechanisms
un-mixable across an upgrade.

One live action is nonetheless **retained**, and only because it has no argv home: the operator's
`SENTINEL SET <master> auth-pass` push (`littlered_controller.go:1048`, `:1134`, `:1154`,
`:1977`). It is already idempotent and re-issued every pass, so a restarted Sentinel is corrected
within one pass — and during E1/R1–R2 the *previous* value is still accepted anyway, which is
what makes the residual harmless. **Its discarded error should stop being discarded** (WP1).

### Q4 — The shared substrate — is it as large as the brief thinks?

**Half of it is not needed at all; the other half is bigger than stated.**

**(i) "The operator talking to a mixed-credential fleet — six call sites, one password per pass,
no 'try both'." NOT REQUIRED.** Two independent reasons:

- In the **enablement** direction there is no mismatch to begin with — go-redis's implicit
  `AUTH default <pw>` is accepted by a `nopass` pod (§0, F1d, measured).
- In the **rotation** direction the R1–R3 window means every pod accepts the operator's password
  at every instant.

So the eleven `getRedisPassword` call sites, the gatherer, the sentinel/cluster clients and the
two monitors **do not change**. This is a real simplification and it should be stated loudly,
because "teach the gather to try both credentials" is the obvious first design and it would
touch the ground truth — which LR-038 warns changes every rule at once.

Three small, genuinely-required fixes remain on this side:

- `getRedisPassword` must **distinguish "auth is off" from "I could not read the Secret"** and
  the caller must surface the second as a condition rather than dialling with `""`. Today a
  transient Secret read failure reads the entire fleet as unreachable, which is §3.5's misfire
  catalogue arriving with no auth change at all.
- The **failover monitor**'s `return err == nil` (`failover_monitor.go:253-259`) turns an auth
  error into evidence of master death. It only *accelerates* a reconcile (the decision is
  `planMasterDeath`, which HOLDs), so this is a diagnostic wart rather than a hazard — but it
  should log the distinction.
- The discarded `SENTINEL SET auth-pass` errors (above).

**(ii) "Change detection and rollout sequencing." REQUIRED, and it is the whole substrate.**
Today a password change hashes to nothing and rolls nothing (§3.6). This needs:

- **`AnnotationAuthHash`**, stamped on the pod template in **all four** modes, over the
  *effective credential set and stage* — never over a bare password. Recommendation: HMAC the
  concatenation of the stage name, `password` and `additionalPassword` with the instance's
  `metadata.uid` as the key, and take 16 hex chars. The rationale must be written down: a plain
  SHA-256 of a password placed in a widely-readable object is an offline-crackable artifact, and
  a StatefulSet is readable by anyone with `get statefulsets`.
- **`computePodTemplateHash`/`AnnotationPodSpecHash` extended beyond cluster mode**, *or*
  `AnnotationAuthHash` used directly as the change signal in the other three. The latter is
  smaller and is recommended for the first milestone; extending `PodSpecHash` to sentinel/failover
  is a separate, larger change with its own rollout consequences.
- **A stage renderer**: one pure function mapping `(spec.auth, secret keys, applied stage)` to
  the stage to render, and one set of builder inputs derived from it.

**Sequencing is *not* required**, which is the design's second free win (§4, property 5).

**Recommendation:** design (ii) once, for all modes, in the first milestone. Do not build (i).

### Q5 — Per-mode edges and the implementation order — CHALLENGED

The brief proposes **standalone → failover → cluster → sentinel**. I agree with the first two and
**recommend swapping the last two: standalone → failover → sentinel → cluster.**

| mode | internal edges | why here |
|---|---|---|
| **1. standalone** | none | Exactly right, and for the reason given: it isolates the substrate. One pod, one restart, no peer to be out of step with. Everything in Q4(ii) is exercised with none of the topology risk. |
| **2. failover** | `masterauth` only; the operator owns every decision; the startup start-gate needs no credential at all (§3.2) | Right. It is the smallest instance of the internal-edge problem, so `masteruser default` and the two-stage renderer are proven before `sentinel-pass`/`auth-pass`/quorum arrive. It also has the strongest existing e2e (a rolling-update tier with data intact, LR-038 addenda). |
| **3. sentinel** ⬅ moved up | `masterauth` + `sentinel-pass` (peer membership) + `auth-pass` (operator-pushed), a **quorum split** while the peers disagree, **and the roll wipes Sentinel's EmptyDir into Rule L** | **This is the mode where the hazard is real** (§3.5: the Rule L reseed, and Sentinel failing over a healthy master on its own). It is also the mode whose runbooks *already tell owners to enable auth* (`docs/USAGE.md`, ADR-015 Decision 6, pillar 3.7). Shipping "auth changes are supported" for three modes while the one mode users will actually do it on is unbuilt is the worse failure. Hard, but it is the point of the feature. |
| **4. cluster** ⬅ moved down | `masterauth`; **the bus is unauthenticated regardless** (F7); 2N pods; LR-021 cross-shard serialization **plus** ADR-017's intra-shard partition gate; `MIGRATE … AUTH` needs `AUTH2` (F7c) | Lowest value and highest cost. `docs/USAGE.md` already says a password *does not* protect the cluster mesh, so auth here is a client-edge nicety. And the cost is the largest: **two or three fully serialized rollouts, each shard gated on a full sync** — for a large dataset that is measured in tens of minutes and the user must be warned (LR-047's own "Regresses" note makes the same point about rollout duration). It also carries the only genuine code fix outside the substrate (WP6, `AUTH2`). |

Two per-mode edges worth naming that the brief did not:

- **Failover mode's start gate is credential-free** (`resources_failover.go:329-331`), so a
  failover pod restarted mid-stage parks or starts on annotations alone. The LR-038 epoch/marker
  machinery is untouched by this feature. That is a genuine simplification and should be checked
  rather than assumed (WP4).
- **Cluster mode emits `--requirepass "${REDIS_PASSWORD}"` unconditionally**
  (`resources.go:2195`), so the *argv shape* is identical whether auth is on or off, and the
  template diff on an enable comes entirely from the env entry and the probe flags. Any
  stage renderer for cluster mode must not assume the flag's presence signals anything.

### Q6 — Interactions with what was just built

**(a) Enabling auth changes the Sentinel pod template, so the Sentinel roll wipes EmptyDir into
Rule L.** Confirmed and it is worse than "lands in Rule L": with the naive single-stage change
it lands in Rule L's **no-data-reseed** branch, which needs no opt-in and elects an empty
`redis-0` over a live dataset (§3.5). With the staged design the Redis pods are always reachable
to the operator, so `RealMasterIP != ""` and Rule L's precondition never holds; the returning
bare Sentinels are picked up by **Rule 0**, which is the designed path and the one M4a observed.
**Residual, accepted:** the sentinel StatefulSet rolls fast (its probes are a bare `PING`,
`resources.go:1596`/`:1610`), so all three Sentinels can be bare simultaneously for a few
seconds — a window with no monitoring, hence no failover. That is exactly §6.2's rejected
"roll the Sentinel StatefulSet" cost, arriving here unavoidably because the credential *is* in
the Sentinel template. It is availability-only and bounded by the roll; it belongs in the
runbook, not in a guard.

**(b) A pod that cannot authenticate reads as unreachable, and unreachability drives
`planForsaken`, the quarantine's data clauses, and failover's death detection.** Traced in §3.5.
The staged design removes the input rather than guarding the consumers. For the record of what
would have happened otherwise: `planForsaken` clause 4 is satisfied the wrong way by an
unreachable master; the quarantine's `unverified` is **not** fooled (LR-044 rekeyed it onto
kubelet readiness, LR-023); failover's death detection **HOLDs** (the LR-017 corroboration
clause); and Rule L is the one that actually destroys something. **A botched auth change looks
exactly like a leaderless deadlock, not like a capture** — worth stating precisely, because the
brief's guess was "or like a capture" and the capture reading is blocked by clause 3 (the foreign
address must not be one of our pods, and here every address *is* one of ours).

**(c) Does LR-050's rollout attribution gate cover an auth rollout? — MOSTLY YES, and the gap is
named.**

- **It fires.** `rolling` is `!statefulSetRolloutSettled(<the Redis StatefulSet>)`, read uncached
  (`littlered_controller.go:913-928`), and the predicate is deliberately broader than "a template
  rollout is in flight" — *any* pod short of Ready fails `ReadyReplicas == Replicas`
  (`cluster_rollout.go:57-75`, LR-050's own wording). An auth-stage rollout changes the Redis pod
  template and takes pods down, so the gate holds attribution for the whole window. **That is
  substantial de-risking obtained for free**, and it means an auth rollout cannot *arm* a false
  `Forsaken` verdict or a false Rule N `Foreign` accusation.
- **The gap: the gate reads only the *Redis* StatefulSet.** An auth change also rolls the
  **Sentinel** StatefulSet, and there is a window in which the Redis STS has settled while the
  Sentinel STS has not. In practice the Redis roll is far longer (`minReadySeconds: 35` per pod
  against a bare-PING Sentinel probe), so the Redis STS settles *last* and the gap is empty for
  this feature — but that is a timing argument, and **LR-050's own lesson is that a margin
  against a user-settable timer is not a design** (`minReadySeconds` is user-settable, pinned by
  `TestRenameFixtureDoesNotOverrideMinReadySeconds` for exactly this reason). Recommendation:
  extend `rolling` to be the **OR over both** of the instance's StatefulSets in sentinel mode.
  One extra uncached GET per sentinel pass, no new RBAC, and it makes the gate say what it
  means. Recorded as **WP2b** and as risk **K7**.
- **What the gate does *not* cover:** it suppresses attribution only. Rule L, Rule D, Rule 0,
  Rule R and failover's death detection all still run during an auth rollout — which is correct,
  and is why the staged design (rather than a wider gate) is the fix.

---

## 6. Alternatives considered — kept with their reasons and their reopening triggers

### 6.1 Single-stage change (flip the template once and let it roll) — REJECTED

The obvious design, and the one a reader arrives with. Rejected because every intermediate state
is a mixed-credential fleet, which §3.5 shows reaches **Rule L's no-data reseed over a live
dataset** in sentinel mode and makes Sentinel fail over a healthy master on its own. Its only
advantage is one fewer rollout per mode (~3 minutes).
**Trigger to reopen:** none. If the staged renderer proves too complex, the correct fallback is
6.2 (refuse the transition), never this.

### 6.2 Make `spec.auth` immutable (CEL transition rule) — REJECTED, kept as the fallback

Honest and free: an auth change becomes delete-and-recreate, which is what every current runbook
implicitly assumes. Rejected because it forecloses the operation the product's own security
guidance demands (`docs/USAGE.md` step 2, ADR-015 Decision 6, pillar 3.7) and because telling an
owner to destroy a working dataset to obtain the isolation the project *recommends* is a worse
trade than the staging we can build. It is also the exact shape of the rename's §6.1, and the
same answer applies.
**Trigger to reopen: if the staged renderer cannot be made safe, ship immutability rather than
shipping today's silent mixed-credential transition** — because §3.5 is a defect either way.

### 6.3 Live multi-password via `ACL SETUSER` — REJECTED

§13's own idea, and its own objection is confirmed by measurement (F2, F3). Two further nails:
a Sentinel cannot change its incoming requirement live at all (F5b), and every startup script
re-copies the ConfigMap so a live setting does not survive a restart (§3.2).
**Trigger to reopen:** an `aclfile` + `ACL LOAD` design, which would give a genuinely
rollout-free window. Blocked today by the `aclfile`/`user`-directive exclusivity that **exits**
the server (`acl.c:2570-2577`), so it cannot coexist with 6.4's argv form across an upgrade.

### 6.4 Dual-Secret rotation (`existingSecret: a → b`) — REJECTED as the mechanism, retained as a supported manual path

Swapping the Secret *name* does change `secretKeyRef.Name`, hence the pod template, hence rolls —
so it "works" today in the sense that pods pick the new password up. It gives **no overlap**
(one Secret, one password), so it is the single-stage change of 6.1 with extra steps. Retained as
supported: an owner who does it gets the same staged treatment if the new Secret carries the
`additionalPassword` key.
**Trigger to reopen:** if users turn out to manage credentials exclusively through Secret
*names* (e.g. an external-secrets operator that never mutates a Secret in place), promote it to a
first-class path by reading two Secrets instead of two keys. The stage machine is unchanged.

### 6.5 Teach the operator to try both credentials — REJECTED

The brief's Q4 premise. Rejected because §0/F1d and the R1–R3 window make it unnecessary, and
because it would widen the ground truth — LR-038's *"a guard written against the ground truth is
only as good as what the ground truth is allowed to contain"* cuts both ways, and LR-043's
correction is the worked example of a predicate accumulating clauses until it denies a legitimate
own node.
**Trigger to reopen:** if a mode is ever added where the staged renderer cannot cover an edge
(the most likely candidate is an external component the operator does not template).

### 6.6 Keep `masteruser default` / `sentinel-user default` permanently — REJECTED

Simpler (no flag churn between stages) but it permanently makes our replicas willing to sync from
a **password-less** foreign master, which is precisely the address-adoption path ADR-015 says
`masterauth` closes and pillar 3.7 calls *"the only thing closing the narrower address-adoption
path"*. The username form is confined to stage E1, where the instance has no auth at all and so
loses nothing.
**Trigger to reopen:** never on security grounds. If a *client-compatibility* reason emerges,
it is a client-side setting, not a server-side one.

### 6.7 An operator-driven, fully automatic rotation (the operator picks the new password) — REJECTED, out of scope

It would remove the user's two Secret edits, but it requires the operator to *generate* and
*remember* a credential — persisted load-bearing state of the worst kind (a secret), and an
ADR-006 violation with real consequences. Credentials are the platform's to own.

---

## 7. Mechanics, pass by pass

### 7.1 Enablement, happy path (sentinel mode, the hardest)

Preconditions: `Phase: Running`, `Ready=True`, all pods ready, not `Forsaken`, no failover in
flight, the Secret exists with a `password` key. Clients configured with the password and using a
username-capable AUTH form (§Q1).

- **t0** — the owner sets `spec.auth.enabled: true` (and `existingSecret`).
- **Pass 1** — `validateSpec` passes. The stage resolver sees: desired = enforce (E2), applied =
  E0 (no `AnnotationAuthHash` on either StatefulSet). **It renders E1, not E2** — the live
  template is the cursor (LR-047). Both StatefulSets are applied with the E1 template and
  `AnnotationAuthHash = H(E1, …)`. The operator's own password is now P, and every pod is still
  `nopass` → **every probe succeeds** (F1b, measured).
- **t0+0…~3 min** — the Sentinel STS rolls (fast, bare-PING probes; all three may be bare at
  once — see §Q6(a)) and the Redis STS rolls behind `minReadySeconds: 35`, reverse-ordinal, with
  the master last. Rolled Redis pods present `AUTH default P` to whatever master they follow;
  un-rolled pods present nothing to peers that demand nothing. Rolled Sentinels present
  `AUTH default P` to peers and to the master; the master accepts (nopass short-circuit) and a
  `nopass` Sentinel peer *errors on the AUTH and ignores it* (F5) and then answers everything
  normally. Rule 0 re-registers the bare Sentinels; the operator's `SENTINEL SET auth-user
  default` + `auth-pass P` pushes ride the existing per-pass loop.
  **LR-050's gate holds attribution for the entire window** (§Q6(c)).
- **Pass N** — both StatefulSets are settled at `H(E1, …)`. The resolver advances and renders
  **E2**: the `*-user default` flags are dropped and `--requirepass $REDIS_PASSWORD` appears.
- **~3 more minutes** — the second roll. Every pod already presents P, so a rolled pod's new
  demand is satisfied by every peer immediately. Clients that were pre-configured see nothing.
- **End state** — byte-identical to a freshly created auth-on instance. `ConditionAuthReady=True`.

Standalone collapses this to two single-pod restarts. Failover is the same shape with only
`masterauth`. Cluster is the same shape multiplied by LR-021's shard serialization and ADR-017's
partition gate.

### 7.2 Rotation, happy path

- The owner adds `additionalPassword: NEW` to the Secret. The operator's `AnnotationAuthHash`
  changes → **R1 rolls**. Every pod ends up accepting `{OLD, NEW}` (F3b) and presenting OLD.
  The operator keeps using `password` = OLD, which every pod accepts.
- Once R1 is settled the operator **auto-advances to R2**: present NEW, still accept both. It can
  do this without remembering anything, because both values are in the Secret. Second roll.
  The operator now uses NEW — accepted everywhere.
- **Clients migrate any time between the start of R1 and the end of R2**, individually, in any
  order, with any AUTH form.
- The owner removes `additionalPassword` (and has already swapped `password` to NEW, or the
  operator's R2 rendering has already made NEW the presented value — see the open question **O3**
  on exactly which key holds which value at R2). **R3 rolls.** Accept NEW only.

### 7.3 What the operator must NOT do here

- **Never `CONFIG SET requirepass`.** It collapses the accepted set to one (F2) and would
  instantly break the overlap for every peer that has not yet rolled.
- **Never render a `user` directive into a ConfigMap or into a StatefulSet's argv as a literal.**
  It must be built in the startup script from the env (R8).
- **Never render a `user` directive *and* leave `sentinel-pass` unset on a Sentinel.** Sentinel
  falls back to `server.requirepass` for its peer credential, which the `user` form leaves empty
  (F5c).
- **Never advance a stage on the *phase*.** The phase is written at the tail of the pass and lags
  by one (LR-044's M4b finding). Gate on the StatefulSets' applied hash and settledness.
- **Never combine an auth change with a master-name rename** (N9).

### 7.4 Interaction with the quarantine and the rename

- A **quarantined** instance has zero pods, so there is nothing to roll and the resolver must
  treat it as "hold" rather than "settled". `statefulSetRolloutSettled` on a `replicas: 0`
  StatefulSet returns **true** (`0 == 0` on all three counts), which is the trap: a quarantined
  instance would read as settled and the resolver would advance a stage over an empty instance,
  then advance again, "completing" an auth change that no pod ever executed. **Guard: the stage
  resolver must refuse to advance while `status.quarantinedSince` is set.** This is the LR-044
  `ScaleToZero` pre-gather decision arriving in a second consumer, and it is cheap.
- The **rename** (ADR-018) and an auth change both roll the Redis StatefulSet and both interact
  with LR-050's gate. Doing them together makes the LR-050 window longer and makes a failure
  unattributable. N9, and the runbook says so — as `docs/USAGE.md:392-395` already does for the
  reverse direction.

---

## 8. The pure seam

This is the spec sub-agents implement against. Everything below is I/O-free.

```go
// AuthStage is the credential posture a pod template renders.
type AuthStage string

const (
    AuthStageOff      AuthStage = "Off"      // no credential anywhere
    AuthStagePrepare  AuthStage = "Prepare"  // present with the username form; demand nothing
    AuthStageEnforce  AuthStage = "Enforce"  // present; demand `password`
    AuthStageWiden    AuthStage = "Widen"    // demand {password, additionalPassword}; present password
    AuthStageSwitch   AuthStage = "Switch"   // demand {password, additionalPassword}; present the NEW one
)

// AuthStageInput is everything the decision may read. No client, no context.
type AuthStageInput struct {
    Enabled            bool      // spec.auth.enabled
    SecretPresent      bool      // the Secret was read successfully
    HasAdditional      bool      // the Secret carries `additionalPassword`
    AppliedStages      []AuthStage // one per StatefulSet the instance owns, from AnnotationAuthHash
    AllSettled         bool      // every one of those StatefulSets is statefulSetRolloutSettled
    Quarantined        bool      // status.quarantinedSince != nil
}

type AuthStagePlan struct {
    Render AuthStage // what the builders must stamp THIS pass
    Reason string    // "Advancing", "Holding", "Converged", "Quarantined", "SecretUnreadable"
    Detail string    // for the condition message
}

func planAuthStage(in AuthStageInput) AuthStagePlan
```

Rules, and every one of them is a table row:

| # | Condition | Render | Reason |
|---|---|---|---|
| 1 | `Quarantined` | the applied stage, unchanged | `Quarantined` |
| 2 | `!SecretPresent && Enabled` | the applied stage, unchanged | `SecretUnreadable` (and a `Warning` event) |
| 3 | `!Enabled` and applied is `Off` | `Off` | `Converged` |
| 4 | `Enabled`, applied is `Off` | **`Prepare`** | `Advancing` |
| 5 | `Enabled`, applied is `Prepare`, `!AllSettled` | `Prepare` | `Holding` |
| 6 | `Enabled`, applied is `Prepare`, `AllSettled` | **`Enforce`** | `Advancing` |
| 7 | `Enabled`, applied is `Enforce`, `!HasAdditional` | `Enforce` | `Converged` |
| 8 | `Enabled`, applied is `Enforce`, `HasAdditional` | **`Widen`** | `Advancing` |
| 9 | `Enabled`, applied is `Widen`, `!AllSettled` | `Widen` | `Holding` |
| 10 | `Enabled`, applied is `Widen`, `AllSettled` | **`Switch`** | `Advancing` |
| 11 | `Enabled`, applied is `Switch`, `HasAdditional` | `Switch` | `Converged` (waiting for the user to drop the key) |
| 12 | `Enabled`, applied is `Switch`, `!HasAdditional` | **`Enforce`** | `Advancing` (R3) |
| 13 | applied stages **disagree** across StatefulSets | the **lowest** applied stage | `Holding` |
| 14 | `!Enabled`, applied is `Enforce`/`Widen`/`Switch` | **`Prepare`** (then `Off`) | `Advancing` — the disable mirror, N6 |

Row 13 is what makes sentinel mode's two independent StatefulSets safe without sequencing: the
instance's stage is the **minimum** of its parts, so no builder can ever render a stage that
another StatefulSet has not yet reached.

Row 1's `Quarantined` and row 2's `SecretUnreadable` are both *holds*, never *advances* — R6.

**Mutation checks the table must survive** (LR-043/LR-044 precedent): an "always advance" mutant
must fail rows 1, 2, 5, 9, 11 and 13; an "always hold" mutant must fail rows 4, 6, 8, 10, 12 and
14. Both directions, or the table has no teeth.

**A second, smaller seam:**

```go
// authArgs renders the shell fragment for a stage. Pure: it emits variable
// REFERENCES, never a password.
func authArgs(stage AuthStage, kind podKind) authFragment
```
with `podKind ∈ {redis, sentinel}` and `authFragment` carrying `ServerArgs`, `CliArgs` and
`ExporterEnv`. Its table pins, per stage and kind, exactly which of `--requirepass`,
`--masterauth`, `--masteruser`, `--sentinel sentinel-pass`, `--sentinel sentinel-user` and
`--user default on '>…' '>…' '~*' '&*' +@all` appear — and that **no literal `$REDIS_PASSWORD`
value** appears in any of them.

---

## 9. Work packages

Ordered by dependency. Disjoint file ownership is noted so siblings can run in parallel.

| WP | What | Owns | Depends on |
|---|---|---|---|
| **WP0** | **Observation, build nothing.** Reproduce §3.5's Rule L reseed on a throwaway sentinel instance: enable auth in one step, sample `status`, the operator log and `DBSIZE` at 1s. Exit criterion: either the `Reseeded` verdict is observed (the design's red is banked) **or** it is not, in which case §3.5's chain must be re-derived before anything is built. This is the equivalent of the rename's WP0 and it must be done **first**. | nothing | — |
| **WP1** | `getRedisPassword` returns `(string, error)`; a Secret read failure becomes a `Warning` + condition instead of `""`. Un-discard the four `SENTINEL SET auth-pass` errors. Log the auth-vs-timeout distinction in the failover monitor. | `littlered_controller.go`, `failover_monitor.go` | — (independently valuable; ship it even if the feature slips) |
| **WP2** | The pure seam: `planAuthStage` + `authArgs`, red-first, both mutants. | `internal/controller/auth_stage_plan.go` + tests | — |
| **WP2b** | Extend LR-050's `rolling` to the OR over both sentinel StatefulSets (§Q6(c)). Red-first with a fixture where the Redis STS is settled and the Sentinel STS is not. | `littlered_controller.go` | — |
| **WP3** | The substrate: `AnnotationAuthHash` (HMAC-keyed on the instance UID), stamped in all four modes; the stage resolver reading applied hashes uncached; `AuthReady` condition + events. | `resources.go` (hash helper), the four apply sites | WP2 |
| **WP4** | **standalone + failover** builders wired to `authArgs`. Assert the failover start gate stays credential-free. | `resources.go` (standalone), `resources_failover.go` | WP3 |
| **WP5** | **sentinel** builders: data pods and sentinel pods, both stages, plus `auth-user` in the operator's `SENTINEL SET` push. | `resources.go` (sentinel), `littlered_controller.go` (push) | WP4 |
| **WP6** | **cluster** builders; **and change `MIGRATE … AUTH <pw>` to `AUTH2 default <pw>`** (F7c) so a reshard survives a mixed fleet. | `resources.go` (cluster), `internal/redis/cluster_migrate.go` | WP5 |
| **WP7** | `lrctl`: report the applied stage per pod and **fail** when the pods disagree with the CR. `lrctl` is the ground-truth tool here (§3.4). | `cmd/lrctl`, `internal/cli` | WP3 |
| **WP8** | e2e (§10). | `test/e2e` | WP4–WP6 |
| **WP9** | Docs: ADR-019, the LR-nnn entry, `docs/USAGE.md` runbooks, `docs/API_SPEC.md`, `CLAUDE.md` pillar 3.7 and §4. | docs | all |

---

## 10. Verification plan

| Tier | What | Where the red comes from |
|---|---|---|
| Observation | **WP0**: the single-step enable reproduces Rule L's reseed | it *is* the red, and it is the decision gate for §6.1 |
| Unit (pure) | `planAuthStage` 14-row table + both mutants | authored against a zero-value stub, red on every row |
| Unit (pure) | `authArgs` per stage × kind; **plus a guard that no rendered string contains a literal password** | red before the renderer exists |
| Unit (builders) | each mode's builder at each stage; a guard that `AnnotationAuthHash` changes on a Secret-content change and **not** on an unrelated spec edit | red — today the hash does not exist |
| Unit (regression) | the entire existing `planForsaken`, `planQuarantine`, `planLeaderlessRecovery` and `planMasterDeath` tables must pass **with no row edited** (LR-048's K2b stop condition) | n/a — it is a stop condition, not a red |
| envtest | a CR flipped to `auth.enabled: true` renders `Prepare`, holds while unsettled, then renders `Enforce` | red before WP3 |
| e2e — enablement | per mode: enable on a **running instance holding data**; assert data intact, no `Forsaken`, no `Reseeded`, no spurious failover, `Ready=True` throughout, and a client with the password + username form never fails | red against HEAD (WP0's shape) |
| e2e — rotation | sentinel + failover: `additionalPassword` → settle → auto-`Switch` → drop the key; assert **both** passwords authenticate throughout the window and exactly one does at the end; data intact | red against HEAD (no rollout happens at all today) |
| e2e — negative | a Secret whose `password` key is deleted mid-stage must produce `SecretUnreadable` and **hold**, not advance | red |
| Manual | `lrctl verify` before / mid / after | — |

**Environment:** t3e or s1. Record the measured duration of each stage per mode; **replace** the
estimates in this document with them rather than leaving both (LR-044's M4a precedent).

---

## 11. Runbook (draft — the shipped copy belongs in `docs/USAGE.md`)

### Enabling authentication on a running instance

**Preconditions.** `Phase: Running`, `Ready=True`, all pods ready; not `Forsaken`; no failover in
flight; the platform stable. **Your clients must already hold the password and must authenticate
with a username** (`AUTH default <pw>`, `AUTH2`, or `HELLO 3 AUTH default <pw>`). If they cannot,
plan a client cutover at step 4 instead.

1. `kubectl create secret generic <name>-auth --from-literal=password='<pw>'`
2. Confirm health: `lrctl status <n>` and `lrctl verify <n>`.
3. `kubectl patch littlered <n> --type=merge -p '{"spec":{"auth":{"enabled":true,"existingSecret":"<name>-auth"}}}'`
4. Watch `kubectl get littlered <n> -o jsonpath='{.status.conditions[?(@.type=="AuthReady")]}'`.
   It will report `Prepare` → `Enforce` → `True`. Two rollouts; expect roughly **2 × the normal
   rolling-update time for your mode** (measure it — see §10).
5. Verify: `lrctl verify <n>`, then `redis-cli -h <svc> --user default -a '<pw>' PING`.

**What you will see, and it is expected:** the Sentinel pods cycle quickly and may all be bare
for a few seconds during each roll — there is no monitoring, hence no failover, in that window.
The Redis pods roll one at a time behind `minReadySeconds`, master last.

**Do not:** combine this with a `masterName` rename; do it on a degraded or `Forsaken` instance;
or try to shortcut it by editing the StatefulSet by hand (server-side apply with
`ForceOwnership` will overwrite you on the next pass).

### Rotating the password

1. Add the new password **beside** the old one:
   `kubectl patch secret <name>-auth --type=merge -p '{"stringData":{"additionalPassword":"<new>"}}'`
2. Wait for `AuthReady` to report `Switch` and settle. **From here until step 4 both passwords
   work, for every client and every AUTH form.**
3. Migrate your clients to the new password, one at a time.
4. Remove the old one:
   `kubectl patch secret <name>-auth --type=json -p '[{"op":"remove","path":"/data/additionalPassword"}]'`
   and set `password` to the new value if it is not already.
5. Verify: the old password must now be refused, the new one accepted.

**If you abandon a rotation midway**, remove `additionalPassword` and leave `password` at the
value your clients currently use. The operator narrows back to one password on the next roll.

---

## 12. Risk register

| # | Risk | Severity | Mitigation / decision |
|---|---|---|---|
| **K1** | A single-stage auth change reaches Rule L's no-data reseed and elects an empty master over a live dataset (§3.5). | **Critical** — data loss on a supported operation | The staged design removes the input. WP0 banks the red first, and the e2e tier asserts data intact. Note this is a *present* defect, reachable today by any user who edits `spec.auth`. |
| **K2** | Sentinel fails over a healthy master while the quorum disagrees about credentials. | High | Same mitigation; E1 is inert and E2's demand is always already satisfiable. |
| **K3** | An owner performs an auth change and a `masterName` rename in the same window and cannot attribute a failure. | Medium | N9, the runbook, and `docs/USAGE.md:392-395`'s existing sentence generalized. |
| **K4** | A password literal leaks into a StatefulSet, ConfigMap or Event. | High | R8 + the pure `authArgs` guard test asserting no rendered string contains a value; `AnnotationAuthHash` is an HMAC keyed on the instance UID, never a bare digest. |
| **K5** | ⚠ **A `user` directive in argv is FATAL if the same directive is also in the config file the process reads** (F3c, measured). Today this cannot happen because every startup script `cp`s the ConfigMap over the writable copy on every start — an unrelated, undocumented coupling. | Medium, latent | Add an explicit comment at each `cp` site saying it is load-bearing for the auth renderer, plus a builder test asserting the `cp` precedes the `exec`. If anyone ever makes a config file persistent or the copy conditional, the Sentinel pods crash-loop at startup. |
| **K6** | The stage resolver advances over a **quarantined** instance, because a `replicas: 0` StatefulSet reads as `statefulSetRolloutSettled == true`. | Medium | §7.4: row 1 of the table refuses to advance while `status.quarantinedSince` is set. Pinned by a table row. |
| **K7** | LR-050's gate reads only the Redis StatefulSet, so a Sentinel-only unsettled window is unattributed (§Q6(c)). | Low today (a timing argument), Medium in principle | WP2b widens `rolling` to the OR over both. LR-050's own lesson — a margin against a user-settable timer is not a design — applies to the timing argument being relied on otherwise. |
| **K8** | Cluster-mode auth changes take two or three fully serialized rollouts, each shard gated on a full sync (ADR-017), which for a large dataset is tens of minutes and will be reported as a hang. | Medium, cosmetic | Documented in the runbook with the same framing LR-047 used for its own rollout-duration regression. Cluster is last in the order (Q5) partly for this. |
| **K9** | `MIGRATE … AUTH <pw>` (two-argument) fails against a `nopass` destination, so a reshard in flight during a cluster enablement stalls. | Low | WP6 changes it to `AUTH2 default <pw>`. Note ASM (Redis 8.4+) is unaffected — it uses the cluster-internal secret (F7b). |
| **K10** | `getRedisPassword` returning `""` on a Secret read error makes the operator read an enforcing fleet as entirely unreachable — §3.5's catalogue with no auth change at all. | High, pre-existing | WP1, shippable independently. |
| **K11** | The `*-user default` flags, if ever left in place past stage E1, let a replica sync from a password-less foreign master — reopening ADR-015's address-adoption path. | High if it happens | §6.6; pinned by an `authArgs` table row asserting the flags appear **only** in `Prepare`. |
| **K12** | Enabling auth silently changes the LR-044 quarantine attempt budget from N=1 to N=2 (`quarantineConfigDangerous`, `littlered_controller.go:2287-2290`). | Low | Correct behaviour; documented in the runbook so it is not discovered during an incident. |

---

## 13. Open questions that need a live experiment

Flagged rather than guessed, per the brief. **None of these was settled, and none should be
assumed in an implementation.**

| # | Question | The experiment that settles it |
|---|---|---|
| **O1** | **Does §3.5's Rule L reseed actually fire?** The chain is traced from four independently-verified facts (gather maps a credential error to `Reachable:false`; `DataHolders` filters on `Reachable`; the Sentinel STS rolls faster than the Redis STS; the 30s cooldown is far shorter than a roll) but has **never been observed**. It has exactly the shape ADR-016's "the captor heals via Rule D" had — an inference from independently-documented mechanisms that turned out correct but was only *known* after M4a. | **WP0.** A throwaway sentinel instance holding data; enable auth in one step; sample `status.conditions`, the operator log and `DBSIZE` at 1s across the whole window. Exit criterion: the `LeaderlessRecovery=…/Reseeded` line, or its absence with an explanation. |
| **O2** | **How long does each stage actually take, per mode?** Every duration in this document is derived from `minReadySeconds: 35` and the existing rollout measurements (the rename measured 176.8s for three sentinel Redis pods), not measured for this feature. Cluster mode's number is genuinely unknown because ADR-017's gate makes it dataset-dependent. | Time each stage on t3e for all four modes, with and without a non-trivial dataset. Replace §7's estimates (LR-044 M4a precedent). |
| **O3** | **Which key holds which value at stage `Switch`?** §4's table has the user swapping `password` and `additionalPassword` at R2, while §7.2 has the operator auto-advancing to `Switch` without a user edit. Those two cannot both be right. The clean resolution is that `Switch` renders `additionalPassword` as the *presented* value and both as accepted — but that makes the key names lie for one stage. Decide before WP2; it is a naming/semantics question, not a mechanism question. | No experiment needed — an owner decision. Listed here because it is a genuine gap in this design, not a detail. |
| **O4** | **Does an un-rolled Sentinel presenting no credential to an enforcing peer merely lose the link, or does it also lose its `+switch-master` hello propagation?** F5 establishes the command link fails; the hello channel is published *to* peers, so it should fail with it, but Sentinel also learns peers via the master's hello channel, which may still work. This only matters for the rejected single-stage design, so it is low priority. | A three-Sentinel lab with one enforcing and two not, watching `num-other-sentinels` and `SENTINEL is-master-down-by-addr` responses. |
| **O5** | **Does anything in the exporter sidecar break mid-stage?** `redis_exporter` reads `REDIS_PASSWORD` from the env and its behaviour against a `nopass` server with a password configured was not checked. If it sends bare `AUTH`, metrics go dark for the duration of stage E1. | Run `oliver006/redis_exporter` against a `nopass` server with `REDIS_PASSWORD` set and check whether `redis_up` stays 1. |
| **O6** | **Is `HELLO … AUTH default <pw>` accepted by every version in the supported range** (Redis 7.2+, Valkey 8)? Verified by source at 8.4.2 and Valkey 8.1 and measured at 8.4.2 only. The `nopass` short-circuit is ancient (Redis 6.0 ACLs) so this is very likely fine, but "very likely" is not this project's standard. | Repeat the go-redis probe matrix (§2, F1d) against `redis:7.2`, `redis:8.4.2` and `valkey:8.1` containers. Ten minutes, no cluster. |
| **O7** | **Does `--user default …` in argv behave identically on Valkey?** Confirmed by source ordering (`valkey/src/server.c:2981` vs `:7171`, `acl.c:2229-2232`) but **not** executed. | One `valkey:8.1` container, the same F3b command line. |

---

## 14. Definition of done

1. Enabling auth on a **running, data-holding** instance of each of the four modes leaves the
   data intact, produces no `Forsaken` verdict, no `Reseeded` recovery and no spurious failover —
   proven by e2e on a real cluster, with WP0's pre-fix RED recorded.
2. Rotating a password on a sentinel and a failover instance authenticates **both** passwords
   throughout the overlap and exactly one at the end, with the data intact — proven by e2e, with
   the pre-fix RED ("no rollout happens at all") recorded.
3. Every refusal path names its gate in the `AuthReady` condition message and fires at most one
   event per transition.
4. The **entire** existing `planForsaken` / `planQuarantine` / `planLeaderlessRecovery` /
   `planMasterDeath` tables pass with **no row edited**.
5. No rendered string, in any mode or stage, contains a password literal — pinned by a test.
6. WP0's and O2's measurements are in ADR-019, and §7's estimates have been **replaced** by them
   rather than left standing beside them.
7. ADR-019 and the LR-nnn entry are written; `docs/USAGE.md` carries both runbooks;
   `docs/API_SPEC.md` and `CLAUDE.md` (pillar 3.7, §4) are updated; `make lint && make test` are
   clean.
8. The deferred items are recorded **as decisions with triggers**, not as loose ends:
   immutability as the fallback (§6.2), the `aclfile` reopening trigger (§6.3), dual-Secret
   rotation (§6.4), disabling auth (N6).
