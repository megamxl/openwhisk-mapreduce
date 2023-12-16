package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/jthomas/ow"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Params struct {
	Payload string `json:"payload"`
}

type CONT struct {
	BucketName   string `json:"bucketName"`
	Key          string `json:"key"`
	OutputBucket string `json:"outputBucket"`
}

type Result struct {
	Key  string  `json:"key"`
	Time float64 `json:"time"`
}

type ValueData struct {
	Value int `json:"value"`
}

func main() {
	ow.RegisterAction(func(value json.RawMessage) (interface{}, error) {
		var cc CONT
		err := json.Unmarshal(value, &cc)
		if err != nil {
			return nil, err
		}

		endpoint := "192.168.178.250:9000"
		accessKeyID := "minioadmin"
		secretAccessKey := "minioadmin"

		minioClient, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
			Secure: false,
		})
		if err != nil {
			log.Fatalln(err)
		}

		var amount int
		startTime := time.Now()

		// Listing objects
		objectCh := minioClient.ListObjects(context.TODO(), cc.BucketName, minio.ListObjectsOptions{
			Prefix:    "key/" + cc.Key,
			Recursive: true,
		})

		// Iterating over the listed objects
		for object := range objectCh {
			if object.Err != nil {
				log.Fatalln(object.Err)
			}

			// Getting object data
			object, err := minioClient.GetObject(context.TODO(), cc.BucketName, object.Key, minio.GetObjectOptions{})
			if err != nil {
				log.Fatalln(err)
			}
			data, err := ioutil.ReadAll(object)
			if err != nil {
				log.Fatalln(err)
			}

			var line ValueData
			err = json.Unmarshal(data, &line)
			if err != nil {
				log.Fatalln(err)
			}

			amount += line.Value
		}

		// Creating output object
		outputData, err := json.Marshal(map[string]int{cc.Key: amount})
		if err != nil {
			log.Fatalln(err)
		}
		_, err = minioClient.PutObject(context.TODO(), cc.OutputBucket, cc.Key, bytes.NewReader(outputData), int64(len(outputData)), minio.PutObjectOptions{
			ContentType: "application/json",
		})
		if err != nil {
			log.Fatalln(err)
		}

		endTime := time.Now()

		myResult := Result{
			Key:  cc.Key,
			Time: endTime.Sub(startTime).Seconds(),
		}

		return myResult, err
	})
}

func randomString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	rand.Seed(time.Now().UnixNano())
	s := make([]rune, length)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
