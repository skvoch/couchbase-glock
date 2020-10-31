package main

import (
	"github.com/couchbase/gocb"
	
	"github.com/rs/zerolog/log"
	"time"
)

const (
	defaultCbTimeout = time.Second * 5
	defaultCbHost = "127.0.0.1"
	defaultCbLogin = "Administrator"
	defaultCbPassword = "password"
	defaultCbBucketName = "items"
	defaultGoroutinesCount = 10

	defaultLockDuration = time.Second * 15
	defaultGoroutineWaitDuration = time.Second * 10
)

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
			locker := cb_glock

			for {
				time.Sleep(defaultGoroutineWaitDuration)




			}
		}()
	}
}
