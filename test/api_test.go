package test

import (
	"context"
	"encoding/json"
	"testing"

	proto "github.com/micro-in-cn/config-srv/proto/config"
	"github.com/micro/go-micro/client"
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
		Comment: "adding ios phones",
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
		Comment: "adding ios phones",
		ChangeSet: &proto.ChangeSet{
			Data:   dataBytes,
			Format: "json",
		},
	}})
	if err != nil {
		t.Errorf("[TestUpdate] create error %s", err)
		return
	}

	t.Logf("[TestUpdate] create result %s", rsp)
}

func TestUpdateD(t *testing.T) {
	greeter := proto.NewConfigService("go.micro.srv.config", client.DefaultClient)

	data := map[string]interface{}{
		"d": map[string]interface{}{
			"name": "im d",
			"e": map[string]interface{}{
				"name": "im e"}},
	}

	dataBytes, _ := json.Marshal(data)
	t.Logf("[TestUpdateD] update data %s", dataBytes)

	rsp, err := greeter.Update(context.TODO(), &proto.UpdateRequest{Change: &proto.Change{
		Id:      "NAMESPACE:CONFIG",
		Path:    "a/b/c/d",
		Author:  "shuxian",
		Comment: "adding ios phones",
		ChangeSet: &proto.ChangeSet{
			Data:   dataBytes,
			Format: "json",
		},
	}})
	if err != nil {
		t.Errorf("[TestUpdateD] create error %s", err)
		return
	}

	t.Logf("[TestUpdateD] create result %s", rsp)
}
