package handler

import (
	"strings"
	"time"

	"github.com/micro-in-cn/config-srv/config"
	"github.com/micro-in-cn/config-srv/db"
	proto "github.com/micro-in-cn/config-srv/proto/config"
	"github.com/micro/go-micro/config/source"
	"github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/util/log"
	"golang.org/x/net/context"
)

type Config struct{}

func (c *Config) Read(ctx context.Context, req *proto.ReadRequest, rsp *proto.ReadResponse) (err error) {
	if len(req.Id) == 0 {
		err = errors.BadRequest("go.micro.srv.config.Read", "invalid id")
		log.Error(err)
		return err
	}

	ch, err := db.Read(req.Id)
	if err != nil {
		err = errors.BadRequest("go.micro.srv.config.Read", "read error: %s", err)
		log.Error(err)
		return err
	}
	// Set response
	rsp.Change = ch

	// if dont need path, we return all of the data
	if len(req.Path) == 0 {
		rsp.Change.Path = ""
		rsp.Change.Timestamp = 0
		return nil
	}

	rsp.Change.Path = req.Path

	values, err := config.Values(&source.ChangeSet{
		Timestamp: time.Unix(ch.ChangeSet.Timestamp, 0),
		Data:      ch.ChangeSet.Data,
		Checksum:  ch.ChangeSet.Checksum,
		Format:    ch.ChangeSet.Format,
		Source:    ch.ChangeSet.Source,
	})
	if err != nil {
		return errors.InternalServerError("go.micro.srv.config.Read", err.Error())
	}

	parts := strings.Split(req.Path, config.PathSplitter)

	// we just want to pass back bytes
	rsp.Change.ChangeSet.Data = values.Get(parts...).Bytes()

	return nil
}

func (c *Config) Create(ctx context.Context, req *proto.CreateRequest, rsp *proto.CreateResponse) (err error) {
	if req.Change == nil || req.Change.ChangeSet == nil {
		err = errors.BadRequest("go.micro.srv.config.Create", "invalid change")
		log.Error(err)
		return err
	}

	if len(req.Change.Id) == 0 {
		err = errors.BadRequest("go.micro.srv.config.Create", "invalid id")
		log.Error(err)
		return err
	}

	req.Change.Timestamp = time.Now().Unix()
	req.Change.ChangeSet.Timestamp = time.Now().Unix()

	// Set the change at a particular path
	// Since its a create request we have to build the path
	if len(req.Change.Path) > 0 {
		// Unpack the data as a go type
		var data interface{}
		vals, err := config.Values(&source.ChangeSet{
			Data:   req.Change.ChangeSet.Data,
			Format: req.Change.ChangeSet.Format,
		})
		if err != nil {
			err = errors.InternalServerError("go.micro.srv.config.Create", "values error: %s", err)
			log.Error(err)
			return err
		}
		if err := vals.Get().Scan(&data); err != nil {
			err = errors.InternalServerError("go.micro.srv.config.Create", "scan data error: %s", err)
			log.Error(err)
			return err
		}

		// Create the new change
		newChange, err := config.Merge(&source.ChangeSet{Data: vals.Bytes(), Format: req.Change.ChangeSet.Format})
		if err != nil {
			return errors.InternalServerError("go.micro.srv.config.Create", err.Error())
		}

		req.Change.ChangeSet = &proto.ChangeSet{
			Timestamp: newChange.Timestamp.Unix(),
			Data:      newChange.Data,
			Checksum:  newChange.Checksum,
			Source:    newChange.Source,
			Format:    newChange.Format,
		}
	}

	if err := db.Create(req.Change); err != nil {
		err = errors.BadRequest("go.micro.srv.config.Create", "create new into db error: ", err)
		log.Error(err)
		return err
	}

	config.Publish(ctx, &proto.WatchResponse{Id: req.Change.Id, ChangeSet: req.Change.ChangeSet})

	return nil
}

func (c *Config) Update(ctx context.Context, req *proto.UpdateRequest, rsp *proto.UpdateResponse) (err error) {
	if req.Change == nil || req.Change.ChangeSet == nil {
		err = errors.BadRequest("go.micro.srv.config.Update", "invalid change")
		log.Error(err)
		return err
	}

	if len(req.Change.Id) == 0 {
		err = errors.BadRequest("go.micro.srv.config.Update", "invalid id")
		log.Error(err)
		return err
	}

	req.Change.Timestamp = time.Now().Unix()
	req.Change.ChangeSet.Timestamp = time.Now().Unix()

	// Get the current change set
	ch, err := db.Read(req.Change.Id)
	if err != nil {
		err = errors.BadRequest("go.micro.srv.config.Update", "read old value error: ", err)
		log.Error(err)
		return err
	}

	change := &source.ChangeSet{
		Timestamp: time.Unix(ch.ChangeSet.Timestamp, 0),
		Data:      ch.ChangeSet.Data,
		Checksum:  ch.ChangeSet.Checksum,
		Source:    ch.ChangeSet.Source,
		Format:    ch.ChangeSet.Format,
	}

	var newChange *source.ChangeSet

	// Set the change at a particular path
	if len(req.Change.Path) > 0 {
		// Unpack the data as a go type
		var data interface{}
		vals, err := config.Values(&source.ChangeSet{Data: req.Change.ChangeSet.Data, Format: ch.ChangeSet.Format})
		if err != nil {
			err = errors.InternalServerError("go.micro.srv.config.Update", "values error: %s", err)
			log.Error(err)
			return err
		}
		if err := vals.Get().Scan(&data); err != nil {
			err = errors.InternalServerError("go.micro.srv.config.Update", "scan data error: %s", err)
			log.Error(err)
			return err
		}

		// Get values from existing change
		values, err := config.Values(change)
		if err != nil {
			err = errors.InternalServerError("go.micro.srv.config.Update", "get values from existing change error: %s", err)
			log.Error(err)
			return err
		}

		// Apply the data to the existing change
		// values.Set(data, strings.Split(req.Change.Path, config.PathSplitter)...)

		// Create a new change
		newChange, err = config.Merge(&source.ChangeSet{Data: values.Bytes()})
		if err != nil {
			err = errors.InternalServerError("go.micro.srv.config.Update", "create a new change error: %s", err)
			log.Error(err)
			return err
		}
	} else {
		// No path specified, business as usual
		newChange, err = config.Merge(change, &source.ChangeSet{
			Timestamp: time.Unix(req.Change.ChangeSet.Timestamp, 0),
			Data:      req.Change.ChangeSet.Data,
			Checksum:  req.Change.ChangeSet.Checksum,
			Source:    req.Change.ChangeSet.Source,
			Format:    req.Change.ChangeSet.Format,
		})
		if err != nil {
			err = errors.BadRequest("go.micro.srv.config.Update", "merge all error: ", err)
			log.Error(err)
			return err
		}
	}

	// update change set
	req.Change.ChangeSet = &proto.ChangeSet{
		Timestamp: newChange.Timestamp.Unix(),
		Data:      newChange.Data,
		Checksum:  newChange.Checksum,
		Source:    newChange.Source,
		Format:    req.Change.ChangeSet.Format,
	}

	if err := db.Update(req.Change); err != nil {
		err = errors.BadRequest("go.micro.srv.config.Update", "update into db error: ", err)
		log.Error(err)
		return err
	}

	_ = config.Publish(ctx, &proto.WatchResponse{Id: req.Change.Id, ChangeSet: req.Change.ChangeSet})

	return nil
}

// current implementation of Delete blows away the record completely if Change.ChangeSet.Data is not supplied
func (c *Config) Delete(ctx context.Context, req *proto.DeleteRequest, rsp *proto.DeleteResponse) error {
	if req.Change == nil {
		return errors.BadRequest("go.micro.srv.config.Update", "invalid change")
	}

	if len(req.Change.Id) == 0 {
		return errors.BadRequest("go.micro.srv.config.Update", "invalid id")
	}

	if req.Change.ChangeSet == nil {
		req.Change.ChangeSet = &proto.ChangeSet{}
	}

	if req.Change.Timestamp == 0 {
		req.Change.Timestamp = time.Now().Unix()
	}

	if req.Change.ChangeSet.Timestamp == 0 {
		req.Change.ChangeSet.Timestamp = time.Now().Unix()
	}

	// We're going to delete the record as we have no path and no data
	if len(req.Change.Path) == 0 {
		if err := db.Delete(req.Change); err != nil {
			return errors.InternalServerError("go.micro.srv.config.Delete", err.Error())
		}
		return nil
	}

	// We've got a path. Let's update the required path

	// Get the current change set
	ch, err := db.Read(req.Change.Id)
	if err != nil {
		return errors.InternalServerError("go.micro.srv.config.Delete", err.Error())
	}

	// Get the current config as values
	values, err := config.Values(&source.ChangeSet{
		Timestamp: time.Unix(ch.ChangeSet.Timestamp, 0),
		Data:      []byte(ch.ChangeSet.Data),
		Checksum:  ch.ChangeSet.Checksum,
		Source:    ch.ChangeSet.Source,
	})
	if err != nil {
		return errors.InternalServerError("go.micro.srv.config.Delete", err.Error())
	}

	// Delete at the given path
	// values.Del(strings.Split(req.Change.Path, config.PathSplitter)...)

	// Create a change record from the values
	change, err := config.Merge(&source.ChangeSet{Data: values.Bytes()})
	if err != nil {
		return errors.InternalServerError("go.micro.srv.config.Delete", err.Error())
	}

	// Update change set
	req.Change.ChangeSet = &proto.ChangeSet{
		Timestamp: change.Timestamp.Unix(),
		Data:      change.Data,
		Checksum:  change.Checksum,
		Source:    change.Source,
	}

	if err := db.Update(req.Change); err != nil {
		return errors.InternalServerError("go.micro.srv.config.Delete", err.Error())
	}

	config.Publish(ctx, &proto.WatchResponse{Id: req.Change.Id, ChangeSet: req.Change.ChangeSet})

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
		return errors.InternalServerError("go.micro.srv.config.Search", err.Error())
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
		return errors.BadRequest("go.micro.srv.config.Watch", "invalid id")
	}

	watch, err := config.Watch(req.Id)
	if err != nil {
		return errors.InternalServerError("go.micro.srv.config.Watch", err.Error())
	}
	defer watch.Stop()

	for {
		ch, err := watch.Next()
		if err != nil {
			stream.Close()
			return errors.InternalServerError("go.micro.srv.config.Watch", err.Error())
		}

		if err := stream.Send(ch); err != nil {
			stream.Close()
			return errors.InternalServerError("go.micro.srv.config.Watch", err.Error())
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
		return errors.InternalServerError("go.micro.srv.config.AuditLog", err.Error())
	}

	rsp.Changes = logs

	return nil
}
