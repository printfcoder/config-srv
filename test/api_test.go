package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	proto "github.com/micro-in-cn/config-srv/proto/config"
	"github.com/micro/go-micro/client"
)

var (
	configSrv = proto.NewConfigService("go.micro.srv.config", client.DefaultClient)
)

func TestCreate(t *testing.T) {
	greeter := proto.NewConfigService("go.micro.srv.config", client.DefaultClient)

	data := map[string]interface{}{
		"a": map[string]interface{}{"name": "im a",
			"b": map[string]interface{}{
				"name": "im b",
				"c": map[string]interface{}{
					"name": "im c",
					"d":    map[string]interface{}{"name": "im d"}}}},
	}

	dataBytes, _ := json.Marshal(data)
	t.Logf("[TestCreate] create data %s", dataBytes)

	rsp, err := greeter.Create(context.TODO(), &proto.CreateRequest{Change: &proto.Change{
		Id:      "NAMESPACE:CONFIG",
		Path:    "supported_phones",
		Author:  "shuxian",
		Comment: "create",
		ChangeSet: &proto.ChangeSet{
			Data:   dataBytes,
			Format: "json",
		},
	}})
	if err != nil {
		t.Errorf("[TestCreate] create error %s", err)
		return
	}

	t.Logf("[TestCreate] create result %s", rsp)
}

func TestRead(t *testing.T) {
	greeter := proto.NewConfigService("go.micro.srv.config", client.DefaultClient)

	rsp, err := greeter.Read(context.TODO(), &proto.ReadRequest{
		Id: "NAMESPACE:CONFIG"})
	if err != nil {
		t.Errorf("[TestRead] read error %s", err)
		return
	}

	t.Logf("[TestRead] read result %s", rsp)
}

func TestReadAB(t *testing.T) {
	greeter := proto.NewConfigService("go.micro.srv.config", client.DefaultClient)

	rsp, err := greeter.Read(context.TODO(), &proto.ReadRequest{
		Id:   "NAMESPACE:CONFIG",
		Path: "a/b",
	})
	if err != nil {
		t.Errorf("[TestRead] read error %s", err)
		return
	}

	t.Logf("[TestRead] read result %s", rsp)
}

func TestUpdate(t *testing.T) {
	greeter := proto.NewConfigService("go.micro.srv.config", client.DefaultClient)

	data := map[string]interface{}{
		"a": map[string]interface{}{"name": "im a",
			"b": map[string]interface{}{
				"name": "im b",
				"c": map[string]interface{}{
					"name": "im c",
					"d": map[string]interface{}{
						"name": "im d",
						"e": map[string]interface{}{
							"name": "im e"}}}}},
	}

	dataBytes, _ := json.Marshal(data)
	t.Logf("[TestUpdate] update data %s", dataBytes)

	rsp, err := greeter.Update(context.TODO(), &proto.UpdateRequest{Change: &proto.Change{
		Id:      "NAMESPACE:CONFIG",
		Author:  "shuxian",
		Comment: "update",
		ChangeSet: &proto.ChangeSet{
			Data:   dataBytes,
			Format: "json",
		},
	}})
	if err != nil {
		t.Errorf("[TestUpdate] update error %s", err)
		return
	}

	t.Logf("[TestUpdate] update result %s", rsp)
}

func TestUpdateC(t *testing.T) {
	data := map[string]interface{}{
		"d": map[string]interface{}{
			"name": "im d too2",
			"e": map[string]interface{}{
				"name": "im e too2"}},
	}

	dataBytes, _ := json.Marshal(data)
	t.Logf("[TestUpdateC] update data %s", dataBytes)

	rsp, err := configSrv.Update(context.TODO(), &proto.UpdateRequest{Change: &proto.Change{
		Id:      "NAMESPACE:CONFIG",
		Path:    "a/b/c",
		Author:  "shuxian",
		Comment: "update path c",
		ChangeSet: &proto.ChangeSet{
			Data:   dataBytes,
			Format: "json",
		},
	}})
	if err != nil {
		t.Errorf("[TestUpdateC] update error %s", err)
		return
	}

	t.Logf("[TestUpdateC] update result %s", rsp)
}

func TestDeleteD(t *testing.T) {
	greeter := proto.NewConfigService("go.micro.srv.config", client.DefaultClient)

	rsp, err := greeter.Delete(context.TODO(), &proto.DeleteRequest{Change: &proto.Change{
		Id:      "NAMESPACE:CONFIG",
		Path:    "a/b/c/d",
		Author:  "shuxian",
		Comment: "delete d",
	}})
	if err != nil {
		t.Errorf("[TestDeleteD] delete error %s", err)
		return
	}

	t.Logf("[TestDeleteD] delete result %s", rsp)
}

func TestDelete(t *testing.T) {
	greeter := proto.NewConfigService("go.micro.srv.config", client.DefaultClient)

	rsp, err := greeter.Delete(context.TODO(), &proto.DeleteRequest{Change: &proto.Change{
		Id:      "NAMESPACE:CONFIG",
		Author:  "shuxian",
		Comment: "delete",
	}})
	if err != nil {
		t.Errorf("[TestDelete] delete error %s", err)
		return
	}

	t.Logf("[TestDelete] delete result %s", rsp)
}

func TestSearch(t *testing.T) {
	rsp, err := configSrv.Search(context.TODO(), &proto.SearchRequest{
		Id:     "NAMESPACE:CONFIG",
		Author: "shuxian",
		Limit:  1,
		Offset: 0,
	})
	if err != nil {
		t.Errorf("[TestSearch] search error %s", err)
		return
	}

	t.Logf("[TestSearch] search result %s", rsp)
}

func TestWatch(t *testing.T) {
	var errCh chan error

	// watch
	go func() {
		rsp, err := configSrv.Watch(context.TODO(), &proto.WatchRequest{
			Id: "NAMESPACE:CONFIG",
		})

		if err != nil {
			t.Errorf("[TestWatch] begin to watch error %s", err)
			errCh <- err
			return
		}

		for {
			msg, err := rsp.Recv()
			if err != nil {
				t.Errorf("[TestWatch] watch Recv error %s", err)
				errCh <- err
				return
			}
			t.Logf("[TestWatch] watch result %s", msg)
		}
	}()

	// wait for the Watch connected
	time.Sleep(time.Second)

	// update
	go TestUpdateC(t)

	time.Sleep(time.Second)
	// delete
	go TestDeleteD(t)

	select {
	case err := <-errCh:
		t.Errorf("[TestWatch] watch error %s", err)
		return
	case <-time.After(5 * time.Second):
	}
}

func TestAuditLog(t *testing.T) {
	rsp, err := configSrv.AuditLog(context.TODO(), &proto.AuditLogRequest{
		From:   1578757517,
		To:     1578761783,
		Limit:  5,
		Offset: 0,
	})

	if err != nil {
		t.Errorf("[TestAuditLog] query log error error %s", err)
		return
	}

	t.Logf("[TestAuditLog] search result %s", rsp.Changes)
}
