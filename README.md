# crude

ConnectRPC extension (via Protocol Buffers) to add event lifecycle hooks on CRUD applications.

```proto
service MyRecord {
  // Observe events for all RPCs in this service.
  option (crude.service.enable) = true;

  rpc ReadRecord(ReadRecordRequest) returns (ReadRecordResponse) {
    // Explicitly disable events from this RPC.
    option (crude.rpc.enable) = false;
  };

  rpc UpdateRecord(UpdateRecordRequest) returns (UpdateRecordResponse) {};
}

message UpdateRecordRequest {
  string id = 1;
  string password = 2 [(crude.field.redact = true)];
}
```
