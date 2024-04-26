package s3server

import (
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/Cheivin/simple-s3-server/bucket"
	"github.com/Cheivin/simple-s3-server/signature"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	HeaderEtag = "Etag"
)

type LocationConstraint struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:",chardata"`
}

type Server interface {
	GetBucket(bucketName string) (bucket.Bucket, error)
	ServeHTTP(http.ResponseWriter, *http.Request)
}

type server struct {
	bucketFn    func(bucketName string) (bucket.Bucket, error)
	credentials signature.CredentialStore
}

func (s server) GetBucket(bucketName string) (bucket.Bucket, error) {
	return s.bucketFn(bucketName)
}

func WriteError(w http.ResponseWriter, err Error) {
	err.RequestId = w.Header().Get("x-amz-request-id")
	err.HostId = w.Header().Get("x-amz-id-2")
	body, _ := xml.Marshal(err)
	w.WriteHeader(err.httpCode)
	_, _ = w.Write(body)
}

func (s server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			slog.Error("panic", "err", err)
		}
	}()
	if s.credentials != nil {
		err := signature.V4SignVerify(r, s.credentials)
		if !errors.Is(err, signature.ErrNone) {
			WriteError(w, ErrSignature(err))
			return
		}
	}
	switch r.Method {
	case http.MethodHead:
		s.HandleGetObjectMetadata(w, r)
		return
	case http.MethodPut:
		s.HandlePutObject(w, r)
		return
	case http.MethodDelete:
		s.HandleDeleteObject(w, r)
		return
	case http.MethodGet:
		s.HandleGetObject(w, r)
		return
	}
}

func (s server) HandlePutObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		WriteError(w, ErrWith(http.StatusMethodNotAllowed, nil))
		return
	}
	uri := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
	if len(uri) != 2 {
		WriteError(w, ErrWith(http.StatusNotFound, nil))
		return
	}
	defer r.Body.Close()
	object := &bucket.Object{
		Key:         uri[1],
		ContentType: r.Header.Get("Content-Type"),
		Metadata: &bucket.Metadata{
			ModTime: time.Now().UnixMilli(),
		},
		ReadCloser: r.Body,
	}

	// var reader io.Reader
	if sha := r.Header.Get("X-Amz-Content-Sha256"); sha == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		object.ReadCloser = newChunkedReader(r.Body)
		size := r.Header.Get("X-Amz-Decoded-Content-Length")
		if size == "" {
			WriteError(w, ErrWith(http.StatusBadRequest, nil))
			return
		}
	}

	b, err := s.GetBucket(uri[0])
	if err != nil {
		WriteError(w, ErrWith(http.StatusInternalServerError, err))
		return
	}
	metadata, err := b.PutObject(object)
	if err != nil {
		WriteError(w, ErrWith(http.StatusInternalServerError, err))
		return
	}
	w.Header().Set(HeaderEtag, fmt.Sprintf("\"%s\"", metadata.Digest))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte{})
}

func (s server) HandleGetObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrWith(http.StatusMethodNotAllowed, nil))
		return
	}
	uri := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
	if r.URL.Query().Has("location") && len(uri) == 1 {
		w.WriteHeader(http.StatusOK)
		body, _ := xml.Marshal(LocationConstraint{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"})
		_, _ = w.Write(body)
		return
	}
	if len(uri) != 2 {
		WriteError(w, ErrWith(http.StatusNotFound, nil))
		return
	}
	b, err := s.GetBucket(uri[0])
	if err != nil {
		WriteError(w, ErrWith(http.StatusInternalServerError, err))
		return
	}
	object, err := b.GetObject(uri[1])
	if err != nil {
		WriteError(w, ErrWith(http.StatusInternalServerError, err))
		return
	}
	if object == nil {
		WriteError(w, ErrWith(http.StatusNotFound, nil))
		return
	}
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("Last-Modified", time.UnixMilli(object.Metadata.ModTime).UTC().Format(http.TimeFormat))
	w.Header().Set(HeaderEtag, object.Metadata.Digest)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, object)
}

func (s server) HandleDeleteObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		WriteError(w, ErrWith(http.StatusMethodNotAllowed, nil))
		return
	}
	uri := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
	if len(uri) != 2 {
		WriteError(w, ErrWith(http.StatusNotFound, nil))
		return
	}
	b, err := s.GetBucket(uri[0])
	if err != nil {
		WriteError(w, ErrWith(http.StatusInternalServerError, err))
		return
	}
	if err := b.DeleteObject(uri[1]); err != nil {
		WriteError(w, ErrWith(http.StatusInternalServerError, err))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte{})
}

func (s server) HandleGetObjectMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodHead {
		WriteError(w, ErrWith(http.StatusMethodNotAllowed, nil))
		return
	}
	uri := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
	if len(uri) != 2 {
		WriteError(w, ErrWith(http.StatusNotFound, nil))
		return
	}
	b, err := s.GetBucket(uri[0])
	if err != nil {
		WriteError(w, ErrWith(http.StatusInternalServerError, err))
		return
	}
	metadata, err := b.GetObjectMetadata(uri[1])
	if err != nil {
		WriteError(w, ErrWith(http.StatusInternalServerError, err))
		return
	}
	if metadata == nil {
		WriteError(w, ErrWith(http.StatusNotFound, nil))
		return
	}
	w.Header().Set("Content-Type", metadata.ContentType)
	w.Header().Set("Last-Modified", time.UnixMilli(metadata.ModTime).UTC().Format(http.TimeFormat))
	w.Header().Set(HeaderEtag, metadata.Digest)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte{})
}

type FileServer struct {
	server
}

type Option = func(*server)

func WithBucket(fn func(bucketName string) (bucket.Bucket, error)) Option {
	return func(s *server) {
		s.bucketFn = fn
	}
}

func WithAuthStore(credentials signature.CredentialStore) Option {
	return func(s *server) {
		s.credentials = credentials
	}
}

func WithAuth(credentials map[string]string) func(s *server) {
	return func(s *server) {
		if s.credentials == nil {
			s.credentials = &defaultCredentialStore{}
		}
		for accessKey, secretKey := range credentials {
			s.credentials.Put(signature.Credential(accessKey, secretKey))
		}
	}
}

func NewFileServer(basePath string, options ...Option) Server {
	server := server{
		bucketFn: func(bucketName string) (bucket.Bucket, error) {
			return bucket.NewFileBucket(basePath, bucketName, false)
		},
		credentials: nil,
	}
	for i := range options {
		options[i](&server)
	}
	return &FileServer{
		server: server,
	}
}
