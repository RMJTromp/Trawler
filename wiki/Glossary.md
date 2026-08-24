# Glossary

| Term | Meaning |
| --- | --- |
| Admission | The policy decision that determines whether a discovery emission can become accepted work |
| Attempt | One substantive execution of claimed work; lease acquisition is not an attempt |
| Canonicalization | Normalization used before request identity and deduplication |
| Checkpoint | Versioned opaque discovery or harvest progress persisted by the state owner |
| Codec descriptor | Stable ID/version pair used to decode persisted bytes |
| Completion | A terminal run/source condition that passes the selected harvest predicate |
| Deduplication | Policy for suppressing active duplicate work without necessarily suppressing future recrawls |
| Discovery | Extension that emits requests or already-obtained responses |
| Engine run | One finite orchestration result across selected pipelines |
| Extension | Focused capability implementation with optional deterministic lifecycle |
| Fencing token | Lease-owned token that prevents stale workers from mutating reclaimed state |
| Harvest | Recurring-source policy for stateful boundaries and completion |
| Job | One admitted work record for a pipeline/resource |
| Lease | Temporary ownership of a job by a worker |
| Output | Typed sink that acknowledges Produced<T> values |
| Pipeline | Immutable typed composition retaining one T through processing |
| Pipeline run | One pipeline's result within an engine run |
| Policy | Replaceable decision logic for a coordination concern |
| Produced<T> | A typed value plus ProductionContext metadata |
| Replay | Network-free processing of stored responses through a registered typed pipeline |
| Resource | Canonical identity of a crawl target |
| Response store | Optional raw-response retention and lookup capability |
| Retention | Bounded deletion policy for one data category |
| Run recovery | Explicit handling of stale or abandoned durable runs |
| Scope | Policy describing which targets a pipeline may consider |
| Scheduler | Opt-in structured trigger source for engine runs |
| State store | Operational crawl coordination and history owner |
| Transport | Request execution adapter returning core CrawlResponse values |
| Work transition | Explicit terminal or retry outcome for claimed work |

