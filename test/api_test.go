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

	data := map[string][]string{
		"ios": {"4", "5", "6"},
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

func TestUpdate(t *testing.T) {
	greeter := proto.NewConfigService("go.micro.srv.config", client.DefaultClient)

	data := map[string][]string{
		"ios": {"4", "5", "6", "7"},
	}

	dataBytes, _ := json.Marshal(data)
	t.Logf("[TestCreate] create data %s", dataBytes)

	rsp, err := greeter.Update(context.TODO(), &proto.UpdateRequest{Change: &proto.Change{
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
