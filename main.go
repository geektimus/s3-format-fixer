package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type snsTimestamp int64

// Parses object string value to int64
func (ts snsTimestamp) MarshalJSON() ([]byte, error) {
	timestamp := int64(ts)
	millis := time.Unix(0, timestamp).UnixNano()
	b := []byte(strconv.FormatInt(millis, 10))
	return b, nil
}

func (ts *snsTimestamp) UnmarshalJSON(b []byte) error {
	var timestamp string
	err := json.Unmarshal(b, &timestamp)
	if err != nil {
		return err
	}

	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		exitErrorf("Error unmarshaling event timestamp %q", timestamp)
	}
	*ts = snsTimestamp(t.UnixNano() / int64(time.Millisecond))
	return nil
}

type messageAttributes struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Need to ignore this property and parse always as an empty object (JSON)
func (m messageAttributes) MarshalJSON() ([]byte, error) {
	b := []byte(string("{}"))
	return b, nil
}

func (m *messageAttributes) UnmarshalJSON(b []byte) error {
	var messageAttr map[string]string
	err := json.Unmarshal(b, &messageAttr)
	if err != nil {
		return err
	}

	*m = messageAttributes{Key: "", Value: ""}
	return nil
}

type sns struct {
	MessageAttributes *messageAttributes `json:"messageAttributes"`
	SigningCertURL    string             `json:"signingCertUrl"`
	MessageID         string             `json:"messageId"`
	Message           string             `json:"message"`
	UnsubscribeURL    string             `json:"unsubscribeUrl"`
	SnsType           string             `json:"type"`
	SignatureVersion  int                `json:"signatureVersion"`
	Signature         string             `json:"signature"`
	Timestamp         snsTimestamp       `json:"timestamp"`
	TopicArn          string             `json:"topicArn"`
}

type snsEvent struct {
	Sns                  sns     `json:"sns"`
	EventVersion         float32 `json:"eventVersion"`
	EventSource          string  `json:"eventSource"`
	EventSubscriptionArn string  `json:"eventSubscriptionArn"`
}

func exitErrorf(msg string, args ...interface{}) {
	log.Fatalf(msg+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 3 {
		exitErrorf("Bucket name is required\nUsage: go run s3-format-fixer bucket prefix")
	}

	bucket := os.Args[1]
	prefix := os.Args[2]

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	if err != nil {
		exitErrorf("Error trying to create the session")
	}

	// Create S3 service client
	svc := s3.New(sess)

	// List object on the bucket to get the keys
	list, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket), MaxKeys: aws.Int64(10000), Prefix: aws.String(prefix)})
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

		// Create correct json and replace the object on S3

		b, err := json.Marshal(&event)
		if err != nil {
			if e, ok := err.(*json.SyntaxError); ok {
				log.Printf("Syntax error at byte offset %d", e.Offset)
			}
			//log.Printf("Event: %+v", event)
			log.Printf("Error marshaling event: %v", err)
		}

		// Create correct json and replace the object on S3

		result, err := svc.PutObject(&s3.PutObjectInput{
			Body:   aws.ReadSeekCloser(bytes.NewReader(b)),
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})

		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			return
		}

		fmt.Println(result)
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
	return s
}
