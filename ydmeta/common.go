package ydmeta

import (
	"context"
	"errors"
	"fmt"
	"strings"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
)

type KV struct {
	K, V []byte
}

func (kv KV) String() string {
	return fmt.Sprintf("%s => %s (%v)", kv.K, kv.V, kv.V)
}

type MetaManager struct {
	client *txnkv.Client
}

func (m *MetaManager) get(k []byte) ([]byte, error) {
	tx, err := m.client.Begin()
	if err != nil {
		return nil, err
	}
	return tx.Get(context.TODO(), k)
}

func (m *MetaManager) dels(keys ...[]byte) error {
	tx, err := m.client.Begin()
	if err != nil {
		return err
	}
	for _, key := range keys {
		err := tx.Delete(key)
		if err != nil {
			return err
		}
	}
	s := tx.Commit(context.Background())
	return s
}

func (m *MetaManager) set(key []byte, value []byte) error {
	tx, err := m.client.Begin()
	if err != nil {
		return err
	}

	err = tx.Set(key, value)
	if err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

func (m *MetaManager) setIfAbsent(key []byte, value []byte) error {
	tx, err := m.client.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Get(context.TODO(), key)
	if err == nil {
		return fmt.Errorf("key %s has existed", string(key))
	}
	if !errors.Is(err, tikverr.ErrNotExist) {
		return err
	}

	err = tx.Set(key, value)
	if err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

func upper(keyPrefix []byte) []byte {
	if keyPrefix == nil {
		return nil
	}
	keyPrefixUpper := make([]byte, len(keyPrefix))
	copy(keyPrefixUpper, keyPrefix)
	keyPrefixUpper[len(keyPrefixUpper)-1]++
	return keyPrefixUpper
}

func upperWithOffset(keyPrefix []byte, offset int) []byte {
	if keyPrefix == nil {
		return nil
	}
	keyPrefixUpper := make([]byte, len(keyPrefix))
	copy(keyPrefixUpper, keyPrefix)
	keyPrefixUpper[len(keyPrefixUpper)-1] += byte(offset)
	return keyPrefixUpper
}

// list set limit -1 to list without limit
func (m *MetaManager) list(prefix []byte, limit int) ([]KV, error) {
	tx, err := m.client.Begin()
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(prefix, upper(prefix))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() {
		if limit != 0 {
			limit--
		} else {
			break
		}
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		if err = it.Next(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (m *MetaManager) scan(beginKey []byte, endKey []byte, limit int) ([]KV, error) {
	tx, err := m.client.Begin()
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(beginKey, endKey)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() && limit > 0 {
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		limit--
		it.Next()
	}
	return ret, nil
}

func (m *MetaManager) Close() {
	if m.client != nil {
		m.client.Close()
	}
}

func newTikvClient(addrStr string) (*txnkv.Client, error) {
	var addrs []string
	for _, addr := range strings.Split(addrStr, ",") {
		addrs = append(addrs, strings.TrimSpace(addr))
	}

	if len(addrs) <= 0 {
		return nil, errors.New("invalid address")
	}
	return txnkv.NewClient(addrs)
}
