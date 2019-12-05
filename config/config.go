package config

import (
	"encoding/json"
	"errors"
	"hash"
	"sync"
	"time"

	"github.com/imdario/mergo"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/config/reader"
	"github.com/micro/go-micro/config/source"
	proto "github.com/printfcoder/config-srv/proto/config"
	"github.com/pydio/go-os/config"
	"golang.org/x/net/context"
)

var (
	// We need a path splitter since its structured in go-os
	PathSplitter = "/"
	WatchTopic   = "micro.config.watch"

	mtx      sync.RWMutex
	watchers = make(map[string][]*watcher)
)

type watcher struct {
	id   string
	exit chan bool
	next chan *proto.WatchResponse
}

func (w *watcher) Next() (*proto.WatchResponse, error) {
	select {
	case c := <-w.next:
		return c, nil
	case <-w.exit:
		return nil, errors.New("watcher stopped")
	}
}

func (w *watcher) Stop() error {
	select {
	case <-w.exit:
		return errors.New("already stopped")
	default:
		close(w.exit)
	}

	mtx.Lock()
	var wslice []*watcher

	for _, watch := range watchers[w.id] {
		if watch != w {
			wslice = append(wslice, watch)
		}
	}

	watchers[w.id] = wslice
	mtx.Unlock()

	return nil
}

func Parse(changes ...*source.ChangeSet) (*config.ChangeSet, error) {
	var merged map[string]interface{}

	for _, m := range changes {
		if len(m.Data) == 0 {
			m.Data = []byte(`{}`)
		}

		var data map[string]interface{}
		if err := json.Unmarshal(m.Data, &data); err != nil {
			return nil, err
		}
		if err := mergo.Map(&merged, data); err != nil {
			return nil, err
		}
	}

	b, err := json.Marshal(merged)
	if err != nil {
		return nil, err
	}

	h, err := hash.Hash(merged, nil)
	if err != nil {
		return nil, err
	}

	return &config.ChangeSet{
		Timestamp: time.Now(),
		Data:      b,
		Checksum:  fmt.Sprintf("%x", h),
		Source:    "json",
	}, nil
}

func Values(ch *config.ChangeSet) (config.Values, error) {
	return reader.Values(ch)
}

// Watch created by a client RPC request
func Watch(id string) (*watcher, error) {
	mtx.Lock()
	w := &watcher{
		id:   id,
		exit: make(chan bool),
		next: make(chan *proto.WatchResponse),
	}
	watchers[id] = append(watchers[id], w)
	mtx.Unlock()
	return w, nil

}

// Used as a subscriber between config services for events
func Watcher(ctx context.Context, ch *proto.WatchResponse) error {
	mtx.RLock()
	for _, sub := range watchers[ch.Id] {
		select {
		case sub.next <- ch:
		case <-time.After(time.Millisecond * 100):
		}
	}
	mtx.RUnlock()
	return nil
}

// Publish a change
func Publish(ctx context.Context, ch *proto.WatchResponse) error {
	req := client.NewPublication(WatchTopic, ch)
	return client.Publish(ctx, req)
}
