# Pooler signals

How a pooler tells multiorch what it knows and what it wants, and where
multiorch decides what that means. Read this before adding a field that HA
decisions depend on.

## Three layers

| Layer          | Where                                              | What it holds                                                                                            |
| -------------- | -------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Facts          | `AvailabilityStatus.facts` (`AvailabilityFacts`)   | What the pooler observes about itself, such as its postgres status. Raw observations, never conclusions. |
| Intent         | `AvailabilityStatus.intent` (`AvailabilityIntent`) | What the pooler wants, such as a preference not to lead. Each field documents its strength.              |
| Interpretation | multiorch `recovery/types/pooler_signals.go`       | Pure functions that turn facts, intent and `ConsensusStatus` into answers like `CannotActAsLeader`.      |

Poolers report facts and intent. They never report verdicts like "replace me":
multiorch decides what the signals imply. That keeps the policy in one place,
so improving it takes a multiorch upgrade, not a pooler rollout. The
interpretation functions are pure, so a pooler could also run them itself later.

Analyzers and other decision code ask the interpretation layer questions. They
don't read raw signals.

## AvailabilityStatus vs Status

`multipoolermanagerdata.Status` serves many readers (multigateway,
observability, debugging) and may grow freely. `AvailabilityStatus` is the
curated set of pooler signals for HA decisions. Decision logic should need only:

- `ConsensusStatus`: the pooler's rules and position.
- `AvailabilityStatus`: the pooler's facts and intent.
- Topology: how the pooler is configured, such as its leadership mode.
- Multiorch's own observations, such as how recently it heard from the pooler.

Some facts belong in both, such as postgres status. That duplication is
deliberate:

- Fill both copies from one value when building the report, so they can't
  disagree. Test that they match.
- Keep a field's name and type in a copy only if the meaning is identical. If
  decision logic needs a different meaning, give it a different name.

## Adding a signal

1. **Check a decision reads it.** If only dashboards or logs need it, it goes in
   `Status`.
2. **Pick facts or intent.** An observation goes in `facts`. Something the
   pooler wants goes in `intent`. If it reads like a conclusion ("I should be
   replaced"), report the observation behind it instead.
3. **Document intent strength.** Say whether it only orders candidates,
   excludes the pooler, or asks multiorch to act.
4. **Document the zero value.** Older poolers leave the field unset, and it must
   mean "unknown" or "no preference", never something that triggers action.
5. **Interpret it in one place.** Add or update a function in the
   interpretation layer and call it from the analyzer.

## Rolling out a change

Poolers and multiorch upgrade independently, so each change must be safe with
either side older.

- **New signal:** multiorch treats the unset value as "no signal". Behavior
  improves once poolers report it.
- **Moving a signal:** the pooler publishes it in both places, and multiorch
  reads the new one with a fallback to the old. Drop the old one only once no
  supported pooler or multiorch version reads it.
- **Retiring a signal:** stop reading it in multiorch first. Poolers stop
  publishing it in a later release.

## Migration targets

Fields that predate this layout:

- `AvailabilityStatus.cohort_eligibility_status` is intent and should move into
  `intent`.
- `AvailabilityStatus.suspected_divergence` is a fact and should move into
  `facts`.
- `AvailabilityStatus.leadership_status` is no longer published and is read only
  from older poolers.
- Some decision code still reads `Status` directly, for example failover-slot
  readiness and postgres readiness. These should become facts read through the
  interpretation layer as that code is touched.
