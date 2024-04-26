package bucket

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path"
	"time"
)

// Metadata is s3 metadata
type Metadata struct {
	Digest        string `json:"digest"`
	ContentLength int    `json:"content_length"`
	ContentType   string `json:"content_type"`
	ModTime       int64  `json:"mod_time"`
}

type Object struct {
	Key         string
	ContentType string
	Metadata    *Metadata
	io.ReadCloser
}

type Bucket interface {
	PutObject(object *Object) (*Metadata, error)
	DeleteObject(key string) error
	GetObject(key string) (*Object, error)
	PutObjectMetadata(key string, meta *Metadata) (*Metadata, error)
	GetObjectMetadata(key string) (*Metadata, error)
}

type FileBucket struct {
	path        string
	name        string
	directStore bool
}

func NewFileBucket(basePath string, bucketName string, directStore bool) (Bucket, error) {
	bucket := &FileBucket{path: basePath, name: bucketName, directStore: directStore}
	if err := os.MkdirAll(path.Join(basePath, bucketName), 0755); err != nil {
		return nil, err
	}
	if bucket.directStore {
		if err := os.MkdirAll(path.Join(basePath, ".metadata", bucketName), 0755); err != nil {
			return nil, err
		}
	}
	return bucket, nil
}

func (f FileBucket) getObjectFile(key string, create bool) (*os.File, error) {
	keyPath, _ := f.getObjectPath(key)
	keyDir := path.Dir(keyPath)
	if err := os.MkdirAll(keyDir, 0755); err != nil {
		return nil, err
	}
	if create {
		return os.Create(keyPath)
	} else {
		return os.Open(keyPath)
	}
}

func (f FileBucket) getObjectPath(key string) (string, string) {
	if f.directStore {
		return path.Join(f.path, f.name, key), path.Join(f.path, ".metadata", f.name, key)
	} else {
		return path.Join(f.path, f.name, key, ".content"), path.Join(f.path, f.name, key, "metadata.json")
	}
}

func (f FileBucket) PutObject(object *Object) (*Metadata, error) {
	cf, err := f.getObjectFile(object.Key, true)
	if err != nil {
		return nil, err
	}

	defer cf.Close()
	content, err := io.ReadAll(object)
	if err != nil {
		return nil, err
	}

	if _, err := cf.Write(content); err != nil {
		return nil, err
	}
	if err := cf.Sync(); err != nil {
		return nil, err
	}

	hasher := md5.New()
	hasher.Write(content)
	digest := hex.EncodeToString(hasher.Sum(nil))

	return f.PutObjectMetadata(object.Key, &Metadata{
		Digest:        digest,
		ContentType:   object.ContentType,
		ContentLength: len(content),
		ModTime:       time.Now().UnixMilli(),
	})
}

func (f FileBucket) DeleteObject(key string) error {
	keyPath, MetadataPath := f.getObjectPath(key)
	if err := os.Remove(keyPath); err != nil {
		return err
	}
	if err := os.Remove(MetadataPath); err != nil {
		return err
	}
	return nil
}

func (f FileBucket) GetObject(key string) (*Object, error) {
	fc, err := f.getObjectFile(key, false)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	metadata, err := f.GetObjectMetadata(key)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if metadata == nil {
		metadata, err = f.createMetadataFromFile(key)
		if err != nil {
			return nil, err
		}
	}
	return &Object{
		ReadCloser:  fc,
		Key:         key,
		ContentType: metadata.ContentType,
		Metadata:    metadata,
	}, nil
}

func (f FileBucket) createMetadataFromFile(key string) (*Metadata, error) {
	keyPath, _ := f.getObjectPath(key)
	fc, err := os.Open(keyPath)
	if err != nil {
		return nil, err
	}
	fs, err := fc.Stat()
	if err != nil {
		return nil, err
	}
	defer fc.Close()

	hasher := md5.New()
	if _, err := io.Copy(hasher, fc); err != nil {
		return nil, err
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	metadata := &Metadata{
		Digest:        digest,
		ContentType:   "application/octet-stream",
		ContentLength: int(fs.Size()),
		ModTime:       fs.ModTime().UnixMilli(),
	}
	return f.PutObjectMetadata(key, metadata)
}

func (f FileBucket) GetObjectMetadata(key string) (*Metadata, error) {
	_, metadataPath := f.getObjectPath(key)
	mf, err := os.Open(metadataPath)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer mf.Close()
	metadata := new(Metadata)
	meta, err := io.ReadAll(mf)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(meta, metadata); err != nil {
		return nil, err
	}
	return metadata, err
}

func (f FileBucket) PutObjectMetadata(key string, meta *Metadata) (*Metadata, error) {
	metadata, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	_, metadataPath := f.getObjectPath(key)
	if err := os.MkdirAll(path.Dir(metadataPath), 0755); err != nil {
		return nil, err
	}
	if mf, err := os.Create(metadataPath); err != nil {
		return nil, err
	} else {
		defer mf.Close()
		if _, err = mf.Write(metadata); err != nil {
			return nil, err
		}
		if err := mf.Sync(); err != nil {
			return nil, err
		}
	}
	return meta, nil
}
