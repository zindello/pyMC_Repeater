# DIRECT packets not forwarded (router consumes them)

## Summary

Besides ANON_REQ (fixed), these payload types are **always** marked `processed_by_injection` in the router and **never** passed to the engine, so they are **never forwarded** even when this repeater is a middle hop on a DIRECT path:

| Payload type        | Router behavior | Can be DIRECT? | Should forward when middle hop? |
|---------------------|-----------------|-----------------|----------------------------------|
| **ACK**             | Deliver to all companion bridges, set processed | Yes (return path) | Yes |
| **PATH**            | Deliver to companion(s) or all bridges (anon), set processed | Yes (path response) | Yes |
| **LoginResponse**   | Deliver to bridge(s), set processed | Yes (login response) | Yes |
| **ProtocolResponse**| Deliver to companions, set processed | Yes (telemetry etc.) | Yes |
| **Trace**           | trace_helper only, set processed | Yes/No | No (diagnostic, not forwarded by design) |

So **ACK, PATH, LoginResponse (RESPONSE), and ProtocolResponse** are DIRECT packet types that we should be forwarding when we're in the path but currently are not, because the router consumes them every time.

## Detail by type

### ACK (AckHandler)

- **Router:** Delivers to all companion bridges (so sender sees send_confirmed), then sets `processed_by_injection = True`. Never passes to engine.
- **Use case:** ACKs travel back along the path. When we're a middle hop we should forward the ACK toward the sender.
- **Conclusion:** Should be passed to engine after companion delivery so DIRECT ACKs can be forwarded.

### PATH (PathHandler)

- **Router:** If dest in companion_bridges → deliver to that bridge, set processed. If dest not in bridges but we have companions → deliver to all bridges (anon), set processed. Only when path_helper runs (and no companion anon delivery) do we *not* set processed, so the packet can reach the engine.
- **Use case:** PATH responses come back along the path. When we're a middle hop we should forward. When we're final hop (dest in companion or we're path_helper) we should not forward.
- **Conclusion:** When we deliver to companions we still need to pass to engine so we can forward when we're a middle hop. Today we set processed in both companion branches so we never forward PATH.

### LoginResponse (RESPONSE, LoginResponseHandler)

- **Router:** All three branches (dest in companion, dest == local_hash, or anon to all bridges) set `processed_by_injection = True`. Never passes to engine.
- **Use case:** Login responses come back along the path. When we're a middle hop we should forward.
- **Conclusion:** Should be passed to engine after companion delivery so DIRECT RESPONSE can be forwarded.

### ProtocolResponse (ProtocolResponseHandler)

- **Router:** When we have companion_bridges we deliver and set `processed_by_injection = True`. Never passes to engine.
- **Use case:** Protocol responses (telemetry, etc.) come back along the path. When we're a middle hop we should forward.
- **Conclusion:** Should be passed to engine after companion delivery so DIRECT ProtocolResponse can be forwarded.

## Recommended fix (two parts)

### 1. Router: pass to engine after companion delivery

For **ACK, PATH, LoginResponse, ProtocolResponse**, do **not** set `processed_by_injection` so the packet is always passed to the engine after any companion delivery. That implies:

- **ACK:** Remove the unconditional `processed_by_injection = True` (or only set it when we have no companions and are definitely final). Then always pass to engine; engine will forward when we're next hop, drop when "Direct: not for us" or duplicate.
- **PATH:** In both branches where we set processed (dest in companion, anon to all), stop setting `processed_by_injection` so the packet also goes to the engine. Keep companion delivery and `_record_for_ui` as today.
- **LoginResponse:** In all three branches, stop setting `processed_by_injection` so the packet also goes to the engine.
- **ProtocolResponse:** When we have companions, stop setting `processed_by_injection` so the packet also goes to the engine.

We still deliver to companions and call `_record_for_ui` where we do today; we just also pass the packet to the engine so it can forward when we're a middle hop.

### 2. Engine: do not forward when we're final hop (optional but recommended)

In `direct_forward`, after stripping our hash from the path, if the path is empty (hop_count was 1), we're the final destination and should not forward. Today we return the packet and would schedule a transmit with an empty path. Add:

- After `packet.path = bytearray(packet.path[hash_size:])` and updating `path_len`, if `hop_count - 1 == 0` (or `len(packet.path) == 0`), set e.g. `packet.drop_reason = "Direct: final hop (deliver only)"` and return `None` so we don't transmit.

That avoids transmitting when we're the final hop and only delivering to companions.

## Recording for UI

When we pass these packets to the engine, the engine will record them (forwarded or dropped with reason). We can keep calling `_record_for_ui` before passing to the engine for consistency, or rely on the engine's recording; if we do both we might double-record. Prefer: only the engine records when we pass to it (no `_record_for_ui` for these when we're also passing to engine), or keep a single record in the router and don't pass to engine for recording (pass only for forwarding). Simplest is: pass to engine, let engine do the only record (it already builds packet_record for every packet it sees). So we may remove `_record_for_ui` for these four types when we add the "pass to engine" path, to avoid duplicate entries. Alternatively we could keep _record_for_ui and have the engine skip recording when it's a type that the router already recorded—more complex. Easiest: pass to engine, remove the router's _record_for_ui for these four so the engine is the single place that records them.
