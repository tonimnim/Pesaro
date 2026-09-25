package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"strconv"
	"time"

	ledgerv1 "github.com/tonimnim/Pesaro/contracts/gen/ledger/v1"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Authenticator struct{ rules map[string]application.Caller }

func NewAuthenticator(rules map[string]application.Caller) *Authenticator {
	a := &Authenticator{rules: map[string]application.Caller{}}
	for identity, c := range rules {
		copy := application.Caller{Identity: identity, Books: map[domain.ID]bool{}, Permissions: map[string]bool{}, Subjects: map[domain.ID]bool{}}
		for k, v := range c.Books {
			copy.Books[k] = v
		}
		for k, v := range c.Permissions {
			copy.Permissions[k] = v
		}
		for k, v := range c.Subjects {
			copy.Subjects[k] = v
		}
		a.rules[identity] = copy
	}
	return a
}
func (a *Authenticator) authenticate(ctx context.Context) (application.Caller, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return application.Caller{}, status.Error(codes.Unauthenticated, "MTLS_REQUIRED")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return application.Caller{}, status.Error(codes.Unauthenticated, "MTLS_REQUIRED")
	}
	leaf := info.State.PeerCertificates[0]
	if len(leaf.URIs) != 1 {
		return application.Caller{}, status.Error(codes.PermissionDenied, "WORKLOAD_IDENTITY_REQUIRED")
	}
	c, ok := a.rules[leaf.URIs[0].String()]
	if !ok {
		return application.Caller{}, status.Error(codes.PermissionDenied, "WORKLOAD_NOT_ALLOWED")
	}
	return c, nil
}
func validateMessage(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return application.ErrInvalid
	}
	var result error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			return true
		}
		if field.IsList() {
			if field.Kind() == protoreflect.MessageKind {
				list := value.List()
				for i := 0; i < list.Len(); i++ {
					if result = validateMessage(list.Get(i).Message()); result != nil {
						return false
					}
				}
			}
		} else if field.Kind() == protoreflect.MessageKind {
			result = validateMessage(value.Message())
		} else if field.Kind() == protoreflect.StringKind {
			name := string(field.Name())
			if name == "expected_version" || name == "subject_epoch" || name == "account_epoch" {
				n, err := strconv.ParseInt(value.String(), 10, 64)
				if err != nil || n < 0 || value.String() != strconv.FormatInt(n, 10) {
					result = application.ErrInvalid
				}
			}
		}
		return result == nil
	})
	return result
}
func (a *Authenticator) Unary(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
	c, err := a.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if message, ok := request.(proto.Message); !ok || validateMessage(message.ProtoReflect()) != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	ctx, cancel := context.WithTimeout(WithCaller(ctx, c), 15*time.Second)
	defer cancel()
	defer func() {
		if recover() != nil {
			response = nil
			err = status.Error(codes.Internal, "INTERNAL_ERROR_RECOVER_ORIGINAL_OPERATION")
		}
	}()
	return handler(ctx, request)
}

type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedStream) Context() context.Context { return s.ctx }
func (s *authenticatedStream) RecvMsg(message any) error {
	if err := s.ServerStream.RecvMsg(message); err != nil {
		return err
	}
	p, ok := message.(proto.Message)
	if !ok || validateMessage(p.ProtoReflect()) != nil {
		return rpcError(application.ErrInvalid)
	}
	return nil
}
func (a *Authenticator) Stream(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	c, err := a.authenticate(stream.Context())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(WithCaller(stream.Context(), c), 30*time.Second)
	defer cancel()
	return handler(service, &authenticatedStream{ServerStream: stream, ctx: ctx})
}
func NewGRPC(service *application.Service, tlsConfig *tls.Config, rules map[string]application.Caller) (*grpc.Server, error) {
	if tlsConfig == nil || tlsConfig.MinVersion < tls.VersionTLS13 || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.ClientCAs == nil {
		return nil, errors.New("Ledger gRPC requires TLS 1.3 and verified client certificates")
	}
	auth := NewAuthenticator(rules)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig.Clone())),
		grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(1<<20), grpc.MaxConcurrentStreams(64),
		grpc.UnaryInterceptor(auth.Unary), grpc.StreamInterceptor(auth.Stream),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 30 * time.Second}),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 30 * time.Minute, MaxConnectionAgeGrace: time.Minute}))
	ledgerv1.RegisterLedgerServer(server, New(service))
	return server, nil
}
