package web

import (
	"fmt"
	"io"

	"github.com/valyala/fasthttp"
)

const defaultBufSize = 512 * 1024

// Storage defines an interface for the backend storage.
// It can be either on-disk, in-memory, or other types of storage.
type Storage interface {
	Write(msg []byte) error
	Read(off uint64, maxSize uint64, w io.Writer) error
	Ack() error
}

// Server implements a web server
type Server struct {
	s    Storage
	port uint
}

func NewServer(s Storage, port uint) *Server {
	return &Server{s: s, port: port}
}
func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.writeHandler(ctx)
	case "/read":
		s.readHandler(ctx)
	case "/ack":
		s.ackHandler(ctx)
	default:
		ctx.WriteString("Hello world!")
	}
}
func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	if err := s.s.Write(ctx.Request.Body()); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}
func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	off, err := ctx.QueryArgs().GetUint("off")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `off` GET param: %v", err))
		return
	}
	maxSize, err := ctx.QueryArgs().GetUfloat("maxSize")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `maxSize` GET param: %v", err))
		return
	}
	err = s.s.Read(uint64(off), uint64(maxSize), ctx)
	if err != nil && err != io.EOF {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
	if err := s.s.Ack(); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}
func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(":8080", s.handler)
}
