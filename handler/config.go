package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/printfcoder/config-srv/config"
	"github.com/printfcoder/config-srv/db"
	proto "github.com/printfcoder/config-srv/proto/config"
)

type Config struct{}

func (c *Config) Read(ctx context.Context, req *proto.ReadRequest, rsp *proto.ReadResponse) (err error) {
	if len(req.Id) == 0 {
		return fmt.Errorf("[Read] config srv read err: invalid id")
	}

	rsp.Change, err = db.Read(req.Id)
	if err != nil {
		return fmt.Errorf("[Read] config srv read err: %s", err)
	}

	return nil
}

func (c *Config) Create(ctx context.Context, req *proto.CreateRequest, rsp *proto.CreateResponse) (err error) {
	if req.Change == nil || req.Change.ChangeSet == nil {
		return fmt.Errorf("[Create] config srv create err: invalid change")
	}

	if len(req.Change.Id) == 0 {
		return fmt.Errorf("[Create] config srv create err: invalid id")
	}

	if req.Change.Timestamp == 0 {
		req.Change.Timestamp = time.Now().Unix()
	}

	if req.Change.ChangeSet.Timestamp == 0 {
		req.Change.ChangeSet.Timestamp = time.Now().Unix()
	}

	if err := db.Create(req.Change); err != nil {
		return fmt.Errorf("[Create] config srv create err: %s", err)
	}

	_ = config.Publish(ctx, &proto.WatchResponse{Id: req.Change.Id, ChangeSet: req.Change.ChangeSet})

	return nil
}

func (c *Config) Update(ctx context.Context, req *proto.UpdateRequest, rsp *proto.UpdateResponse) error {
	if req.Change == nil || req.Change.ChangeSet == nil {
		return fmt.Errorf("[Update] config srv update err: invalid change")
	}

	if len(req.Change.Id) == 0 {
		return fmt.Errorf("[Update] config srv update err: invalid id")
	}

	if req.Change.Timestamp == 0 {
		req.Change.Timestamp = time.Now().Unix()
	}

	if req.Change.ChangeSet.Timestamp == 0 {
		req.Change.ChangeSet.Timestamp = time.Now().Unix()
	}

	// Get the current change set
	_, err := db.Read(req.Change.Id)
	if err != nil {
		return fmt.Errorf("[Update] config srv read the current change err: %s", err)
	}

	if err := db.Update(req.Change); err != nil {
		return fmt.Errorf("[Update] config srv commit update err: %s", err)
	}

	_ = config.Publish(ctx, &proto.WatchResponse{Id: req.Change.Id, ChangeSet: req.Change.ChangeSet})

	return nil
}

// current implementation of Delete blows away the record completely if Change.ChangeSet.Data is not supplied
func (c *Config) Delete(ctx context.Context, req *proto.DeleteRequest, rsp *proto.DeleteResponse) error {
	if req.Change == nil {
		return fmt.Errorf("[Delete] config srv delete err: invalid change")
	}

	if len(req.Change.Id) == 0 {
		return fmt.Errorf("[Delete] config srv delete err: invalid id")
	}

	if err := db.Delete(req.Change); err != nil {
		return fmt.Errorf("[Delete] config srv delete commit err: %s", err)
	}

	return nil
}

func (c *Config) Search(ctx context.Context, req *proto.SearchRequest, rsp *proto.SearchResponse) error {
	if req.Limit <= 0 {
		req.Limit = 10
	}

	if req.Offset < 0 {
		req.Offset = 0
	}

	changes, err := db.Search(req.Id, req.Author, req.Limit, req.Offset)
	if err != nil {
		return fmt.Errorf("[Search] config srv search err: %s", err)
	}

	for _, ch := range changes {
		ch.Path = ""
		ch.Timestamp = 0
		rsp.Configs = append(rsp.Configs, ch)
	}

	return nil
}

func (c *Config) Watch(ctx context.Context, req *proto.WatchRequest, stream proto.Config_WatchStream) error {
	if len(req.Id) == 0 {
		return fmt.Errorf("[Watch] config srv err: invalid id")
	}

	watch, err := config.Watch(req.Id)
	if err != nil {
		return fmt.Errorf("[Watch] config srv watch err: %s", err)
	}
	defer watch.Stop()

	for {
		ch, err := watch.Next()
		if err != nil {
			stream.Close()
			return fmt.Errorf("[Watch] config srv watch next err: %s", err)
		}

		if err := stream.Send(ch); err != nil {
			stream.Close()
			return fmt.Errorf("[Watch] config srv watch send err: %s", err)
		}
	}
}

func (c *Config) AuditLog(ctx context.Context, req *proto.AuditLogRequest, rsp *proto.AuditLogResponse) error {
	if req.Limit <= 0 {
		req.Limit = 10
	}

	if req.Offset < 0 {
		req.Offset = 0
	}

	if req.From < 0 {
		req.From = 0
	}

	if req.To < 0 {
		req.To = 0
	}

	logs, err := db.AuditLog(req.From, req.To, req.Limit, req.Offset, req.Reverse)
	if err != nil {
		return fmt.Errorf("[AuditLog] config srv err: %s", err)
	}

	rsp.Changes = logs

	return nil
}
