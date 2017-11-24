package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type messageAttributes struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type sns struct {
	MessageAttributes *messageAttributes `json:"messageAttributes"`
	SigningCertURL    string             `json:"signingCertURL"`
	MessageID         string             `json:"messageID"`
	Message           string             `json:"message"`
	UnsubscribeURL    string             `json:"unsubscribeURL"`
	SnsType           string             `json:"type"`
	SignatureVersion  int                `json:"signatureVersion"`
	Signature         string             `json:"signature"`
	Timestamp         string             `json:"timestamp"`
	TopicArn          string             `json:"topicArn"`
}

type snsEvent struct {
	Sns                  sns     `json:"sns"`
	EventVersion         float32 `json:"eventVersion"`
	EventSource          string  `json:"eventSource"`
	EventSubscriptionArn string  `json:"eventSubscriptionArn"`
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 2 {
		exitErrorf("Bucket name is required\nUsage: go run s3-format-fixer bucket")
	}

	bucket := os.Args[1]

	//source := bucket + "/" + item

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	if err != nil {
		exitErrorf("Error trying to create the session")
	}

	// Create S3 service client
	svc := s3.New(sess)

	// List object on the bucket to get the keys
	list, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket), MaxKeys: aws.Int64(10000), Prefix: aws.String("XBO_CI_DEVICE")})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}

	l := make([]string, 0)

	for _, item := range list.Contents {
		l = append(l, *item.Key)
	}

	// Read each object and parse the contents.
	for _, item := range l {
		obj, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &item,
		})
		if err != nil {
			exitErrorf("Unable to read contents of item %q, %v", item, err)
		}
		contents := getContents(obj.Body)

		quotedJSON := parseUnquotedJSON(contents)

		var event snsEvent
		err = json.Unmarshal([]byte(quotedJSON), &event)
		if err != nil {
			exitErrorf("Unable to unmarshal contents of item %q, %v", item, err)
		}
		fmt.Println(event.Sns.Message)
		fmt.Println(event.EventVersion)
		fmt.Println(event.Sns.SignatureVersion)

		//println(contents)
	}
}

func getContents(contents io.ReadCloser) string {
	defer contents.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(contents)
	newStr := buf.String()
	return newStr
}

func parseUnquotedJSON(unquotedJSON string) string {
	// Add quotes to the field names
	var re = regexp.MustCompile(`(['"])?([a-z0-9A-Z_]+)(['"])?:\s`)
	s := re.ReplaceAllString(unquotedJSON, `"$2": `)
	// Add quotes to the values
	re = regexp.MustCompile(`: (['"])?([a-z0-9A-Z_\/\.\-\:\?\&\=\+]+)(['"])?`)
	s = re.ReplaceAllString(s, `: "$2"`)
	// Remove quotes from the numbers (or at least the ones I expect to be numbers)
	re = regexp.MustCompile(`: (["']?)([0-9\.]+)(["']?),`)
	s = re.ReplaceAllString(s, `: $2,`)
	println(s)
	return s
}