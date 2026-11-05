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
  string password = 2 [(crude.field.redact) = true];
}
```

## Usage (`connectrpc.com/connect/v2`)

```go
interceptor := &crude.Interceptor{
	OnEvent: func(ctx context.Context, event crude.Event) error {
		slog.InfoContext(ctx, "rpc observed",
			slog.String("procedure", event.Procedure),
			slog.Any("request", event.Request),
			slog.Any("response", event.Response),
			slog.Any("err", event.Err),
		)
		return nil
	},
}

server := connect.NewServer(interceptor.ServerInterceptor)
```
