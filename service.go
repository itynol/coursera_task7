package main

import (
context "context"
"encoding/json"
	"google.golang.org/grpc/status"

	"log"
"net"
"strings"
"time"

grpc "google.golang.org/grpc"
"google.golang.org/grpc/codes"
"google.golang.org/grpc/metadata"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
const Consumer = "consumer"

type AdminHandler struct {
	ctx context.Context

	//broadcastLogCh основной канал для передачи событий
	broadcastLogCh   chan *Event
	//tspLogListenerChs для доставки каналов с новых подключений в logListeners
	tspLogListenerChs chan chan *Event
	//logListeners для отправки событий во все каналы
	logListeners     []chan *Event

	//Такая же оброботка, как для логирования
	broadcastStatCh   chan *Stat
	tspStatListnerChs chan chan *Stat
	statListeners     []chan *Stat
}

type BizHandler struct {
}

type ACL map[string][]string

type Server struct {
	acl ACL
	AdminHandler
	BizHandler
}

func (srv *Server) multiChanHandlerLog(ctx context.Context) {
	for {
		select {
		case ch := <-srv.tspLogListenerChs:
			srv.logListeners = append(srv.logListeners, ch)
		case event := <-srv.broadcastLogCh:
			for _, ch := range srv.logListeners {
				ch <- event
			}
		case <-ctx.Done():
			return
		}
	}
}


func (srv *Server) multiChanHandlerStats(ctx context.Context) {
	for {
		select {
		case ch := <-srv.tspStatListnerChs:
			srv.statListeners = append(srv.statListeners, ch)
		case event := <-srv.broadcastStatCh:
			for _, ch := range srv.statListeners {
				ch <- event
			}
		case <-ctx.Done():
			return
		}
	}
}

func NewServer(ctx context.Context, acl string) (*Server, error) {
	srv := &Server{
		acl:          nil,
		AdminHandler: AdminHandler{
			ctx:               ctx,
			broadcastLogCh:    make(chan *Event, 0),
			tspLogListenerChs: make(chan chan *Event, 0),
			broadcastStatCh:   make(chan *Stat, 0),
			tspStatListnerChs: make(chan chan *Stat, 0),
		},
	}
	if err := json.Unmarshal([]byte(acl), &srv.acl); err != nil {
		return nil, err
	}
	return srv, nil
}

func StartMyMicroservice(ctx context.Context, addr, acl string) error {
	srv, err := NewServer(ctx, acl)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln(err)
	}

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(srv.streamInterceptor),
		grpc.UnaryInterceptor(srv.unaryInterceptor),
	)
	RegisterBizServer(grpcServer, srv)
	RegisterAdminServer(grpcServer, srv)

	go srv.multiChanHandlerLog(ctx)
	go srv.multiChanHandlerStats(ctx)

	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()

	return nil
}

func (b *BizHandler) Check(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (b *BizHandler) Add(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (b *BizHandler) Test(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (s *Server) checks(ctx context.Context, fullMethod string) error {
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "meta data: get failed")
	}

	consumer, ok := meta[Consumer]
	if !ok || len(consumer) != 1 {
		return status.Errorf(codes.Unauthenticated, "meta data: extract failed")
	}

	allowedPaths, ok := s.acl[consumer[0]]
	if !ok {
		return status.Errorf(codes.Unauthenticated, "allowed paths: get failed")
	}

	splittedMethod := strings.Split(fullMethod, "/")
	if len(splittedMethod) != 3 {
		return status.Errorf(codes.Unauthenticated, "full method path is incorrect")
	}

	methodPath, method := splittedMethod[1], splittedMethod[2]
	isAllowed := false
	for _, allowedPath := range allowedPaths {
		splittedPath := strings.Split(allowedPath, "/")
		if len(splittedPath) != 3 {
			continue
		}
		allowedMethodPath, allowedMethod := splittedPath[1], splittedPath[2]
		if methodPath != allowedMethodPath {
			continue
		}
		if allowedMethod == "*" || method == allowedMethod {
			isAllowed = true
			break
		}
	}
	if !isAllowed {
		return status.Errorf(codes.Unauthenticated, "Method not allowed")
	}
	return nil
}

func (s *Server) unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := s.checks(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "meta data: get failed")
	}

	consumer, ok := meta[Consumer]
	if !ok || len(consumer) != 1 {
		return nil, status.Errorf(codes.Unauthenticated, "meta data: extract failed")
	}

	s.broadcastLogCh <- &Event{
		Consumer: consumer[0],
		Method:   info.FullMethod,
		Host:     "127.0.0.1:8083",
	}
	s.broadcastStatCh <- &Stat{
		ByConsumer: map[string]uint64{consumer[0]: 1},
		ByMethod:   map[string]uint64{info.FullMethod: 1},
	}
	return handler(ctx, req)
}

func (s *Server) streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.checks(ss.Context(), info.FullMethod); err != nil {
		return err
	}

	meta, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return status.Errorf(codes.Unauthenticated, "meta data: get failed")
	}

	consumer, ok := meta[Consumer]
	if !ok || len(consumer) != 1 {
		return status.Errorf(codes.Unauthenticated, "meta data: extract failed")
	}

	s.broadcastLogCh <- &Event{
		Consumer: consumer[0],
		Method:   info.FullMethod,
		Host:     "127.0.0.1:8083",
	}
	s.broadcastStatCh <- &Stat{
		ByConsumer: map[string]uint64{consumer[0]: 1},
		ByMethod:   map[string]uint64{info.FullMethod: 1},
	}
	return handler(srv, ss)
}

func (s *AdminHandler) Logging(nothing *Nothing, srv Admin_LoggingServer) error {
	ch := make(chan *Event, 0)
	s.tspLogListenerChs <- ch

	for {
		select {
		case event := <-ch:
			srv.Send(event)
		case <-s.ctx.Done():
			return nil
		}
	}
	return nil
}

func (s *AdminHandler) Statistics(interval *StatInterval, srv Admin_StatisticsServer) error {
	ch := make(chan *Stat, 0)
	s.tspStatListnerChs <- ch

	ticker := time.NewTicker(time.Second * time.Duration(interval.IntervalSeconds))
	sum := &Stat{
		ByMethod:   make(map[string]uint64),
		ByConsumer: make(map[string]uint64),
	}
	for {
		select {
		case stat := <-ch:
			for k, v := range stat.ByMethod {
				sum.ByMethod[k] += v
			}
			for k, v := range stat.ByConsumer {
				sum.ByConsumer[k] += v
			}

		case <-ticker.C:
			srv.Send(sum)
			sum = &Stat{
				ByMethod:   make(map[string]uint64),
				ByConsumer: make(map[string]uint64),
			}

		case <-s.ctx.Done():
			return nil
		}
	}
	return nil
}