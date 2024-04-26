package s3server

import (
	"simple-s3-server/signature"
	"sync"
)

type defaultCredentialStore struct {
	credStore sync.Map
}

func (d *defaultCredentialStore) Get(accessKey string) (signature.Credentials, bool) {
	v, ok := d.credStore.Load(accessKey)
	if !ok {
		return signature.Credentials{}, false
	}
	return v.(signature.Credentials), true
}

func (d *defaultCredentialStore) Put(credentials signature.Credentials) {
	d.credStore.Store(credentials.AccessKey, credentials)
}
