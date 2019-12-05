package main

import (
	"log"

	"github.com/micro/cli"
	"github.com/micro/go-micro"
	"github.com/printfcoder/config-srv/config"
	"github.com/printfcoder/config-srv/db"
	"github.com/printfcoder/config-srv/db/mysql"
	"github.com/printfcoder/config-srv/handler"
	proto "github.com/printfcoder/config-srv/proto/config"
)

func main() {
	service := micro.NewService(
		micro.Name("go.micro.srv.config"),
		micro.Version("latest"),

		micro.Flags(
			cli.StringFlag{
				Name:   "database_url",
				EnvVar: "DATABASE_URL",
				Usage:  "The database URL e.g root@tcp(127.0.0.1:3306)/trace?charset=utf8&loc=Asia",
			},
		),
		// Add for MySQL configuration
		micro.Action(func(c *cli.Context) {
			if len(c.String("database_url")) > 0 {
				mysql.Url = c.String("database_url")
			}
		}),
	)

	service.Init()

	proto.RegisterConfigHandler(service.Server(), new(handler.Config))

	// subcriber to watches
	service.Server().Subscribe(service.Server().NewSubscriber(config.WatchTopic, config.Watcher))

	if err := db.Init(); err != nil {
		log.Fatal(err)
	}

	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}
