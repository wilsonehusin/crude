package crude

import (
	"context"
	"errors"
	"io"
	"sync"

	"connectrpc.com/connect/v2"
	"go.husin.dev/crude/pb/crudepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Event contains the details of an observed ConnectRPC call.
type Event struct {
	// Spec is the ConnectRPC specification for the invoked procedure.
	Spec connect.Spec
	// Procedure is the RPC procedure path (e.g. "/package.Service/Method").
	Procedure string
	// Method is the Protobuf MethodDescriptor for the RPC.
	Method protoreflect.MethodDescriptor
	// Request is a cloned copy of the RPC request message with fields
	// annotated with [(crude.field.redact) = true] cleared.
	Request proto.Message
	// Response is a cloned copy of the RPC response message with fields
	// annotated with [(crude.field.redact) = true] cleared. It is nil if
	// the RPC failed before producing a response.
	Response proto.Message
	// Err is the error returned by the RPC handler or stream, if any.
	Err error
}

// OnEventFunc is the callback invoked after an observed RPC completes.
// Returning a non-nil error causes the RPC to fail with that error.
type OnEventFunc func(ctx context.Context, event Event) error

// Interceptor observes ConnectRPC calls on services and RPCs annotated
// with (crude.service.enable) or (crude.rpc.enable).
type Interceptor struct {
	OnEvent OnEventFunc
}

// NewInterceptor creates a new Interceptor with the given OnEvent callback.
func NewInterceptor(onEvent OnEventFunc) *Interceptor {
	return &Interceptor{OnEvent: onEvent}
}

// NewServerInterceptor returns a connect.ServerInterceptor that invokes onEvent
// for enabled RPCs.
func NewServerInterceptor(onEvent OnEventFunc) connect.ServerInterceptor {
	ic := &Interceptor{OnEvent: onEvent}
	return ic.ServerInterceptor
}

// NewClientInterceptor returns a connect.ClientInterceptor that invokes onEvent
// for enabled RPCs.
func NewClientInterceptor(onEvent OnEventFunc) connect.ClientInterceptor {
	ic := &Interceptor{OnEvent: onEvent}
	return ic.ClientInterceptor
}

// ServerInterceptor wraps a connect.ServerFunc to observe enabled RPC calls.
func (i *Interceptor) ServerInterceptor(next connect.ServerFunc) connect.ServerFunc {
	return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
		md, enabled := resolveMethod(spec)
		if !enabled || i == nil || i.OnEvent == nil {
			return next(ctx, spec, stream)
		}

		ws := &serverStream{
			ServerStream: stream,
			unary:        spec.StreamType == connect.StreamTypeUnary,
		}
		err := next(ctx, spec, ws)

		event := Event{
			Spec:      spec,
			Procedure: spec.Procedure,
			Method:    md,
			Request:   Redact(ws.req),
			Response:  Redact(ws.res),
			Err:       err,
		}
		if eventErr := i.OnEvent(ctx, event); eventErr != nil {
			if err != nil {
				return errors.Join(err, eventErr)
			}
			return eventErr
		}
		if err != nil {
			return err
		}
		if ws.unary && ws.hasRes {
			return stream.Send(ws.rawRes)
		}
		return nil
	}
}

// ClientInterceptor wraps a connect.ClientFunc to observe enabled RPC calls.
func (i *Interceptor) ClientInterceptor(next connect.ClientFunc) connect.ClientFunc {
	return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
		md, enabled := resolveMethod(spec)
		if !enabled || i == nil || i.OnEvent == nil {
			return next(ctx, spec)
		}

		stream, err := next(ctx, spec)
		if err != nil {
			event := Event{
				Spec:      spec,
				Procedure: spec.Procedure,
				Method:    md,
				Err:       err,
			}
			if eventErr := i.OnEvent(ctx, event); eventErr != nil {
				return nil, errors.Join(err, eventErr)
			}
			return nil, err
		}

		return &clientStream{
			ClientStream: stream,
			ctx:          ctx,
			spec:         spec,
			md:           md,
			onEvent:      i.OnEvent,
		}, nil
	}
}

// IsEnabled reports whether the RPC described by spec has observation enabled
// via (crude.rpc.enable) or (crude.service.enable).
func IsEnabled(spec connect.Spec) bool {
	_, enabled := resolveMethod(spec)
	return enabled
}

func resolveMethod(spec connect.Spec) (protoreflect.MethodDescriptor, bool) {
	md, ok := spec.Schema.(protoreflect.MethodDescriptor)
	if !ok || md == nil {
		return nil, false
	}
	if opts := md.Options(); opts != nil && proto.HasExtension(opts, crudepb.E_Rpc_Enable) {
		enabled, _ := proto.GetExtension(opts, crudepb.E_Rpc_Enable).(bool)
		return md, enabled
	}
	if sd, ok := md.Parent().(protoreflect.ServiceDescriptor); ok && sd != nil {
		if opts := sd.Options(); opts != nil && proto.HasExtension(opts, crudepb.E_Service_Enable) {
			enabled, _ := proto.GetExtension(opts, crudepb.E_Service_Enable).(bool)
			return md, enabled
		}
	}
	return md, false
}

// Redact returns a cloned copy of msg with any fields annotated with
// [(crude.field.redact) = true] cleared. It returns nil if msg is nil or invalid.
func Redact(msg proto.Message) proto.Message {
	if msg == nil || !msg.ProtoReflect().IsValid() {
		return nil
	}
	cloned := proto.Clone(msg)
	redactMessage(cloned.ProtoReflect())
	return cloned
}

func redactMessage(m protoreflect.Message) {
	if !m.IsValid() {
		return
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if isFieldRedacted(fd) {
			m.Clear(fd)
			return true
		}
		switch {
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				list := v.List()
				for i := range list.Len() {
					redactMessage(list.Get(i).Message())
				}
			}
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind || fd.MapValue().Kind() == protoreflect.GroupKind {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					redactMessage(mv.Message())
					return true
				})
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			redactMessage(v.Message())
		}
		return true
	})
}

func isFieldRedacted(fd protoreflect.FieldDescriptor) bool {
	opts := fd.Options()
	if opts == nil || !proto.HasExtension(opts, crudepb.E_Field_Redact) {
		return false
	}
	redacted, _ := proto.GetExtension(opts, crudepb.E_Field_Redact).(bool)
	return redacted
}

type serverStream struct {
	connect.ServerStream

	unary  bool
	req    proto.Message
	res    proto.Message
	rawRes any
	hasRes bool
}

func (s *serverStream) Receive(msg any) error {
	if err := s.ServerStream.Receive(msg); err != nil {
		return err
	}
	if pm, ok := msg.(proto.Message); ok && pm.ProtoReflect().IsValid() {
		s.req = proto.Clone(pm)
	}
	return nil
}

func (s *serverStream) Send(msg any) error {
	if pm, ok := msg.(proto.Message); ok && pm.ProtoReflect().IsValid() {
		s.res = proto.Clone(pm)
	}
	if s.unary {
		s.hasRes = true
		s.rawRes = msg
		return nil
	}
	return s.ServerStream.Send(msg)
}

type clientStream struct {
	connect.ClientStream

	ctx     context.Context
	spec    connect.Spec
	md      protoreflect.MethodDescriptor
	onEvent OnEventFunc

	mu      sync.Mutex
	req     proto.Message
	res     proto.Message
	err     error
	emitted bool
}

func (c *clientStream) Send(msg any) error {
	if pm, ok := msg.(proto.Message); ok && pm.ProtoReflect().IsValid() {
		c.mu.Lock()
		c.req = proto.Clone(pm)
		c.mu.Unlock()
	}
	if err := c.ClientStream.Send(msg); err != nil {
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		return err
	}
	return nil
}

func (c *clientStream) Receive(msg any) error {
	err := c.ClientStream.Receive(msg)
	if err == nil {
		if pm, ok := msg.(proto.Message); ok && pm.ProtoReflect().IsValid() {
			c.mu.Lock()
			c.res = proto.Clone(pm)
			c.mu.Unlock()
		}
		if c.spec.StreamType == connect.StreamTypeUnary {
			return c.emit(nil)
		}
		return nil
	}
	if errors.Is(err, io.EOF) {
		if eventErr := c.emit(nil); eventErr != nil {
			return eventErr
		}
		return err
	}
	if eventErr := c.emit(err); eventErr != nil {
		return errors.Join(err, eventErr)
	}
	return err
}

func (c *clientStream) Close() error {
	closeErr := c.ClientStream.Close()
	c.mu.Lock()
	alreadyEmitted := c.emitted
	recordedErr := c.err
	c.mu.Unlock()

	if !alreadyEmitted {
		rpcErr := recordedErr
		if rpcErr == nil {
			rpcErr = closeErr
		}
		if eventErr := c.emit(rpcErr); eventErr != nil {
			return errors.Join(closeErr, eventErr)
		}
	}
	return closeErr
}

func (c *clientStream) emit(rpcErr error) error {
	c.mu.Lock()
	if c.emitted {
		c.mu.Unlock()
		return nil
	}
	c.emitted = true
	req := c.req
	res := c.res
	c.mu.Unlock()

	event := Event{
		Spec:      c.spec,
		Procedure: c.spec.Procedure,
		Method:    c.md,
		Request:   Redact(req),
		Response:  Redact(res),
		Err:       rpcErr,
	}
	return c.onEvent(c.ctx, event)
}
