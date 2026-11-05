package crude_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/connectinprocess"
	"go.husin.dev/crude"
	examplev1 "go.husin.dev/crude/gen/go/example/v1"
	"go.husin.dev/crude/gen/go/example/v1/examplev1connect"
)

type recordHandler struct {
	examplev1connect.UnimplementedMyRecordServiceHandler
	examplev1connect.UnimplementedUnobservedServiceHandler

	lastPasswordReceived string
	updateErr            error
}

func (h *recordHandler) ReadRecord(_ context.Context, req *examplev1.ReadRecordRequest) (*examplev1.ReadRecordResponse, error) {
	return &examplev1.ReadRecordResponse{
		Id:    req.GetId(),
		Value: "existing-value",
	}, nil
}

func (h *recordHandler) UpdateRecord(_ context.Context, req *examplev1.UpdateRecordRequest) (*examplev1.UpdateRecordResponse, error) {
	h.lastPasswordReceived = req.GetPassword()
	if h.updateErr != nil {
		return nil, h.updateErr
	}
	return &examplev1.UpdateRecordResponse{
		Id:          req.GetId(),
		Value:       req.GetValue(),
		SecretToken: "super-secret-response-token",
	}, nil
}

func (h *recordHandler) Ping(_ context.Context, _ *examplev1.PingRequest) (*examplev1.PingResponse, error) {
	return &examplev1.PingResponse{}, nil
}

func (h *recordHandler) ObservedAction(_ context.Context, req *examplev1.ObservedActionRequest) (*examplev1.ObservedActionResponse, error) {
	return &examplev1.ObservedActionResponse{
		Ok: req.GetName() != "",
	}, nil
}

func TestServerInterceptor_ServiceAndRPCOptions(t *testing.T) {
	var events []crude.Event
	interceptor := &crude.Interceptor{
		OnEvent: func(_ context.Context, event crude.Event) error {
			events = append(events, event)
			return nil
		},
	}

	svc := &recordHandler{}
	server := connect.NewServer(interceptor.ServerInterceptor)
	examplev1connect.RegisterMyRecordServiceHandler(server, svc)
	examplev1connect.RegisterUnobservedServiceHandler(server, svc)

	mux := http.NewServeMux()
	connecthttp.Mount(mux, server)
	httpSrv := httptest.NewServer(mux)
	t.Cleanup(httpSrv.Close)

	transport := connecthttp.NewTransport(httpSrv.Client(), httpSrv.URL, connecthttp.WithHTTPGet())
	connClient := connect.NewClient(transport)
	recordClient := examplev1connect.NewMyRecordServiceClient(connClient)
	unobservedClient := examplev1connect.NewUnobservedServiceClient(connClient)

	ctx := context.Background()

	// 1. ReadRecord has (crude.rpc.enable) = false overriding (crude.service.enable) = true.
	readRes, err := recordClient.ReadRecord(ctx, &examplev1.ReadRecordRequest{Id: "rec-1"})
	if err != nil {
		t.Fatalf("ReadRecord failed: %v", err)
	}
	if readRes.GetId() != "rec-1" {
		t.Fatalf("unexpected ReadRecord response: %+v", readRes)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events after ReadRecord, got %d", len(events))
	}

	// 2. UpdateRecord inherits (crude.service.enable) = true.
	updateReq := &examplev1.UpdateRecordRequest{
		Id:       "rec-1",
		Password: "my-secret-password",
		Value:    "new-value",
		Metadata: &examplev1.Metadata{
			Label:  "primary",
			ApiKey: "secret-key-1",
		},
		History: []*examplev1.Metadata{
			{Label: "v1", ApiKey: "secret-key-2"},
		},
		Tags: map[string]*examplev1.Metadata{
			"env": {Label: "prod", ApiKey: "secret-key-3"},
		},
	}
	updateRes, err := recordClient.UpdateRecord(ctx, updateReq)
	if err != nil {
		t.Fatalf("UpdateRecord failed: %v", err)
	}
	// Verify handler received unredacted request password.
	if svc.lastPasswordReceived != "my-secret-password" {
		t.Fatalf("handler received password %q, want %q", svc.lastPasswordReceived, "my-secret-password")
	}
	// Verify client received unredacted response secret_token.
	if updateRes.GetSecretToken() != "super-secret-response-token" {
		t.Fatalf("client received secret_token %q, want unredacted token", updateRes.GetSecretToken())
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after UpdateRecord, got %d", len(events))
	}

	ev := events[0]
	if ev.Procedure != examplev1connect.MyRecordServiceUpdateRecordProcedure {
		t.Errorf("event.Procedure = %q, want %q", ev.Procedure, examplev1connect.MyRecordServiceUpdateRecordProcedure)
	}
	if ev.Spec.Procedure != examplev1connect.MyRecordServiceUpdateRecordProcedure {
		t.Errorf("event.Spec.Procedure = %q, want %q", ev.Spec.Procedure, examplev1connect.MyRecordServiceUpdateRecordProcedure)
	}
	if ev.Method == nil || ev.Method.Name() != "UpdateRecord" {
		t.Errorf("event.Method = %v, want UpdateRecord descriptor", ev.Method)
	}
	if ev.Err != nil {
		t.Errorf("event.Err = %v, want nil", ev.Err)
	}

	// Verify request redaction in Event.
	observedReq, ok := ev.Request.(*examplev1.UpdateRecordRequest)
	if !ok || observedReq == nil {
		t.Fatalf("event.Request = %T, want *examplev1.UpdateRecordRequest", ev.Request)
	}
	if observedReq.GetId() != "rec-1" || observedReq.GetValue() != "new-value" {
		t.Errorf("non-redacted fields modified: id=%q value=%q", observedReq.GetId(), observedReq.GetValue())
	}
	if observedReq.GetPassword() != "" {
		t.Errorf("expected redacted password to be empty, got %q", observedReq.GetPassword())
	}
	if observedReq.GetMetadata().GetLabel() != "primary" || observedReq.GetMetadata().GetApiKey() != "" {
		t.Errorf("nested metadata redaction failed: %+v", observedReq.GetMetadata())
	}
	if len(observedReq.GetHistory()) != 1 || observedReq.GetHistory()[0].GetLabel() != "v1" || observedReq.GetHistory()[0].GetApiKey() != "" {
		t.Errorf("repeated metadata redaction failed: %+v", observedReq.GetHistory())
	}
	if observedReq.GetTags()["env"].GetLabel() != "prod" || observedReq.GetTags()["env"].GetApiKey() != "" {
		t.Errorf("map metadata redaction failed: %+v", observedReq.GetTags()["env"])
	}

	// Verify response redaction in Event.
	observedRes, ok := ev.Response.(*examplev1.UpdateRecordResponse)
	if !ok || observedRes == nil {
		t.Fatalf("event.Response = %T, want *examplev1.UpdateRecordResponse", ev.Response)
	}
	if observedRes.GetId() != "rec-1" || observedRes.GetValue() != "new-value" {
		t.Errorf("non-redacted response fields modified: id=%q value=%q", observedRes.GetId(), observedRes.GetValue())
	}
	if observedRes.GetSecretToken() != "" {
		t.Errorf("expected redacted secret_token to be empty, got %q", observedRes.GetSecretToken())
	}

	// 3. UnobservedService.Ping has neither service nor RPC option enabled.
	if _, err := unobservedClient.Ping(ctx, &examplev1.PingRequest{}); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected still 1 event after Ping, got %d", len(events))
	}

	// 4. UnobservedService.ObservedAction has (crude.rpc.enable) = true on an otherwise disabled service.
	if _, err := unobservedClient.ObservedAction(ctx, &examplev1.ObservedActionRequest{Name: "deploy"}); err != nil {
		t.Fatalf("ObservedAction failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events after ObservedAction, got %d", len(events))
	}
	if events[1].Procedure != examplev1connect.UnobservedServiceObservedActionProcedure {
		t.Errorf("events[1].Procedure = %q, want %q", events[1].Procedure, examplev1connect.UnobservedServiceObservedActionProcedure)
	}
}

func TestServerInterceptor_HandlerErrorAndOnEventError(t *testing.T) {
	ctx := context.Background()

	t.Run("handler error is observed in OnEvent", func(t *testing.T) {
		var observed []crude.Event
		server := connect.NewServer(crude.NewServerInterceptor(func(_ context.Context, event crude.Event) error {
			observed = append(observed, event)
			return nil
		}))
		svc := &recordHandler{
			updateErr: connect.NewError(connect.CodePermissionDenied, "access denied"),
		}
		examplev1connect.RegisterMyRecordServiceHandler(server, svc)

		client := examplev1connect.NewMyRecordServiceClient(connect.NewClient(connectinprocess.New(server)))
		_, err := client.UpdateRecord(ctx, &examplev1.UpdateRecordRequest{
			Id:       "rec-2",
			Password: "secret",
		})
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("expected CodePermissionDenied, got %v", err)
		}
		if len(observed) != 1 {
			t.Fatalf("expected 1 observed event, got %d", len(observed))
		}
		if connect.CodeOf(observed[0].Err) != connect.CodePermissionDenied {
			t.Errorf("expected observed Err to have CodePermissionDenied, got %v", observed[0].Err)
		}
		if observed[0].Response != nil {
			t.Errorf("expected nil Response on handler error, got %v", observed[0].Response)
		}
		if observed[0].Request.(*examplev1.UpdateRecordRequest).GetPassword() != "" {
			t.Errorf("expected Request password to be redacted even on handler error")
		}
	})

	t.Run("OnEvent error fails the RPC over HTTP", func(t *testing.T) {
		server := connect.NewServer(crude.NewServerInterceptor(func(_ context.Context, _ crude.Event) error {
			return connect.NewError(connect.CodeUnavailable, "event sink unavailable")
		}))
		svc := &recordHandler{}
		examplev1connect.RegisterMyRecordServiceHandler(server, svc)

		mux := http.NewServeMux()
		connecthttp.Mount(mux, server)
		httpSrv := httptest.NewServer(mux)
		t.Cleanup(httpSrv.Close)

		client := examplev1connect.NewMyRecordServiceClient(
			connect.NewClient(connecthttp.NewTransport(httpSrv.Client(), httpSrv.URL)),
		)
		res, err := client.UpdateRecord(ctx, &examplev1.UpdateRecordRequest{
			Id:       "rec-3",
			Password: "secret",
		})
		if err == nil {
			t.Fatalf("expected error when OnEvent fails, got res=%+v", res)
		}
		if connect.CodeOf(err) != connect.CodeUnavailable {
			t.Fatalf("expected CodeUnavailable, got %v (code=%v)", err, connect.CodeOf(err))
		}
	})
}

func TestClientInterceptor(t *testing.T) {
	ctx := context.Background()

	svc := &recordHandler{}
	server := connect.NewServer()
	examplev1connect.RegisterMyRecordServiceHandler(server, svc)

	var clientEvents []crude.Event
	clientInterceptor := crude.NewClientInterceptor(func(_ context.Context, event crude.Event) error {
		clientEvents = append(clientEvents, event)
		return nil
	})

	client := examplev1connect.NewMyRecordServiceClient(
		connect.NewClient(connectinprocess.New(server), clientInterceptor),
	)

	// ReadRecord is disabled.
	if _, err := client.ReadRecord(ctx, &examplev1.ReadRecordRequest{Id: "rec-1"}); err != nil {
		t.Fatalf("ReadRecord failed: %v", err)
	}
	if len(clientEvents) != 0 {
		t.Fatalf("expected 0 client events after ReadRecord, got %d", len(clientEvents))
	}

	// UpdateRecord is enabled.
	res, err := client.UpdateRecord(ctx, &examplev1.UpdateRecordRequest{
		Id:       "rec-1",
		Password: "client-password",
		Value:    "updated",
	})
	if err != nil {
		t.Fatalf("UpdateRecord failed: %v", err)
	}
	if res.GetSecretToken() != "super-secret-response-token" {
		t.Fatalf("unexpected response token: %q", res.GetSecretToken())
	}
	if len(clientEvents) != 1 {
		t.Fatalf("expected 1 client event after UpdateRecord, got %d", len(clientEvents))
	}
	ev := clientEvents[0]
	if ev.Procedure != examplev1connect.MyRecordServiceUpdateRecordProcedure {
		t.Errorf("ev.Procedure = %q, want %q", ev.Procedure, examplev1connect.MyRecordServiceUpdateRecordProcedure)
	}
	if ev.Request.(*examplev1.UpdateRecordRequest).GetPassword() != "" {
		t.Errorf("expected redacted request password in client event")
	}
	if ev.Response.(*examplev1.UpdateRecordResponse).GetSecretToken() != "" {
		t.Errorf("expected redacted response secret_token in client event")
	}

	// Verify client interceptor propagates OnEvent error.
	failingClient := examplev1connect.NewMyRecordServiceClient(
		connect.NewClient(
			connectinprocess.New(server),
			crude.NewClientInterceptor(func(_ context.Context, _ crude.Event) error {
				return errors.New("client sink failed")
			}),
		),
	)
	if _, err := failingClient.UpdateRecord(ctx, &examplev1.UpdateRecordRequest{Id: "rec-1"}); err == nil {
		t.Fatalf("expected error when client OnEvent returns error")
	}
}

func TestLoggerExample(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))

	interceptor := crude.NewInterceptor(func(ctx context.Context, event crude.Event) error {
		logger.InfoContext(ctx, "rpc observed",
			slog.String("procedure", event.Procedure),
			slog.String("method", string(event.Method.Name())),
			slog.Any("request", event.Request),
			slog.Any("response", event.Response),
		)
		return nil
	})

	server := connect.NewServer(interceptor.ServerInterceptor)
	examplev1connect.RegisterMyRecordServiceHandler(server, &recordHandler{})

	client := examplev1connect.NewMyRecordServiceClient(
		connect.NewClient(connectinprocess.New(server)),
	)

	ctx := context.Background()
	// ReadRecord is disabled via (crude.rpc.enable) = false, so nothing is logged.
	if _, err := client.ReadRecord(ctx, &examplev1.ReadRecordRequest{Id: "123"}); err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty log buffer after ReadRecord, got: %s", buf.String())
	}

	// UpdateRecord is observed via (crude.service.enable) = true, with sensitive fields redacted.
	if _, err := client.UpdateRecord(ctx, &examplev1.UpdateRecordRequest{
		Id:       "123",
		Password: "top-secret-password",
		Value:    "hello",
	}); err != nil {
		t.Fatalf("UpdateRecord: %v", err)
	}

	var logEntry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to parse JSON log output (%s): %v", buf.String(), err)
	}
	if logEntry["procedure"] != "/example.v1.MyRecordService/UpdateRecord" {
		t.Errorf("logged procedure = %v, want /example.v1.MyRecordService/UpdateRecord", logEntry["procedure"])
	}
	if bytes.Contains(buf.Bytes(), []byte("top-secret-password")) {
		t.Errorf("log output leaked redacted password: %s", buf.String())
	}
	if bytes.Contains(buf.Bytes(), []byte("super-secret-response-token")) {
		t.Errorf("log output leaked redacted secret_token: %s", buf.String())
	}
}
