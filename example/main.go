package main

import (
	"github.com/couchbase/gocb/v2"
	"github.com/rs/zerolog/log"
	"github.com/skvoch/couchbase-glock"
	"time"
)

const (
	defaultCbTimeout = time.Second * 5
	defaultCbHost = "127.0.0.1"
	defaultCbLogin = "Administrator"
	defaultCbPassword = "password"
	defaultCbBucketName = "items"
	defaultGoroutinesCount = 5

	defaultLockDuration = time.Second * 15
	defaultGoroutineWaitDuration = time.Second * 10
)

type Item struct {
	Name string `json:"name"`
	Price int `json:"price"`
	Count int `json:"count"`
}

func main() {
	cluster, err := gocb.Connect(
		defaultCbHost,
		gocb.ClusterOptions{
			Username: defaultCbLogin,
			Password: defaultCbPassword,
		},
	)

	if err != nil {
		log.Fatal().Err(err).Msgf("failed to connect to cluster")
	}

	bucket := cluster.Bucket(defaultCbBucketName)

	if err = bucket.WaitUntilReady(defaultCbTimeout, nil); err != nil {
		log.Fatal().Err(err).Msgf("failed to wait bucket for ready: %w", err)
	}



	for i := 0; i < defaultGoroutinesCount; i++{
		go func(number int) {
			log.Info().Msgf("im goroutine number %v", number)
			locker,err := cb_glock.New(
				cb_glock.WithBucket(bucket),
				cb_glock.WithCluster(cluster),
				cb_glock.WithLockDuration(defaultLockDuration),
				)

			if err != nil {
				log.Fatal().Err(err).Msg("failed to create locker")
			}


			for {
				var items []Item
				res, err :=  locker.GetAndLock("")

				if err != nil {
					log.Error().Err(err).Msg("getting items")
					continue
				}

				for res.Next() {
					var item Item

					if err := res.Row(&item); err != nil {
						log.Error().Err(err).Msg("scan item")
					}

					items = append(items, item)
				}

				if len(items) > 0{
					log.Info().Int("goroutine", number).Interface("items", items).Msg("")
				}
			}
		}(i)
	}

	for {
		time.Sleep(time.Second * 100)
	}
}
