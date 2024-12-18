package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Constants for S3 and MongoDB
const (
	S3_REGION     = "ap-south-1"
	S3_BUCKET     = "s3 bucket name"
	DB            = "mongo db name"
	COLLECTION    = "mongo collection name"
	WorkerCount   = 100  // Number of workers in the pool for fast upload increase worker too
	ChannelBuffer = 10000 // Size of buffered channel
)

// MongoDB connection function
func mongoConnect() *mongo.Client {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")
	return client
}

// fetch image without watermark
func fetchImageWithoutWatermark(imageUrl string) ([]byte, error) {
	// Define the target URL.
	url := "http://127.0.0.1:4000/remove-watermark"

	fmt.Println("######", imageUrl)

	// Create the JSON payload.
	payload := map[string]string{"image_url": imageUrl}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Create a new POST request.
	req, err := http.NewRequest("POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers.
	req.Header.Set("Content-Type", "application/json")

	// Send the request using an HTTP client.
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer res.Body.Close()

	// Check for non-2xx status codes.
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(res.Body) // Try reading the body for error context.
		return nil, fmt.Errorf("received unexpected HTTP status: %d, body: %s", res.StatusCode, body)
	}

	// Read and return the response body.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}

// Fetch image content from URL
func fetchImageContent(url string) ([]byte, error) {
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch image content: status code %d", res.StatusCode)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// Function Generate UUID
func generateUUID() string {
	newUUID := uuid.New()
	// Convert UUID to string and remove -
	return strings.ReplaceAll(newUUID.String(), "-", "")
}

// todo remove water mark from image

// Upload image to S3
func uploadImageToS3(session *session.Session, bucket, key string, content []byte) (string, error) {
	_, err := s3.New(session).PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(bucket),
		Key:                aws.String(key),
		ACL:                aws.String("public-read"),
		Body:               bytes.NewReader(content),
		ContentLength:      aws.Int64(int64(len(content))),
		ContentType:        aws.String(http.DetectContentType(content)),
		ContentDisposition: aws.String("attachment"),
	})
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucket, S3_REGION, key)
	return url, nil
}

// Worker function
func worker(id int, jobs <-chan *Project, session *session.Session, collection *mongo.Collection, wg *sync.WaitGroup) {
	defer wg.Done()

	for project := range jobs {
		fmt.Printf("Worker %d processing project ID: %s\n", id, project.ID)
		var uploadedUrls []string
		content, err := fetchImageWithoutWatermark(project.TempLink)
		if err != nil {
			log.Printf("Url ==  %s", err)
			continue
		}

		// Extract key from URL
		key := generateUUID()
		s3Key := "images/" + key

		// Upload to S3
		uploadedUrl, err := uploadImageToS3(session, S3_BUCKET, s3Key, content)
		if err != nil {
			log.Printf("Worker %d: Failed to upload to S3 for URL: %s\n", id, project.TempLink)
			continue
		}
		uploadedUrls = append(uploadedUrls, uploadedUrl)

		// Update MongoDB with new URLs
		if len(uploadedUrls) > 0 {
			objID, _ := primitive.ObjectIDFromHex(project.ID)
			filter := bson.M{"_id": objID}
			update := bson.M{"$set": bson.M{"psf_images": uploadedUrls}}

			_, err := collection.UpdateOne(context.TODO(), filter, update)
			if err != nil {
				log.Printf("Worker %d: Failed to update MongoDB for project ID: %s\n", id, project.ID)
			} else {
				fmt.Printf("Worker %d: Successfully updated MongoDB for project ID: %s\n", id, project.ID)
			}
		}
	}
}

// Project structure
type Project struct {
	ID       string `json:"id" bson:"_id"`
	TempLink string `json:"temp_link" bson:"temp_link"`
}

func main() {
	// Set AWS credentials
	os.Setenv("AWS_ACCESS_KEY_ID", "XXXXXXXXXXXXXXXXXXXXXXXXXXX")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "XXXXXXXXXXXXXXXXXXXXXXX")

	// MongoDB connection
	client := mongoConnect()
	collection := client.Database(DB).Collection(COLLECTION)
	defer client.Disconnect(context.TODO())

	// AWS S3 session
	sess, err := session.NewSession(&aws.Config{Region: aws.String(S3_REGION)})
	if err != nil {
		log.Fatal("Failed to create AWS session:", err)
	}

	// Query MongoDB for projects with empty psf_images
	cursor, err := collection.Find(context.TODO(), bson.M{"images": bson.M{"$size": 0}}, options.Find().SetLimit(50000))
	if err != nil {
		log.Fatal("Failed to query MongoDB:", err)
	}
	defer cursor.Close(context.TODO())

	// Create worker pool
	var wg sync.WaitGroup
	jobs := make(chan *Project, ChannelBuffer)

	// Start workers
	for i := 1; i <= WorkerCount; i++ {
		wg.Add(1)
		go worker(i, jobs, sess, collection, &wg)
	}

	// Feed jobs into the channel
	for cursor.Next(context.TODO()) {
		var project Project
		if err := cursor.Decode(&project); err != nil {
			log.Printf("Failed to decode project: %v", err)
			continue
		}
		jobs <- &project
	}

	// Close the jobs channel and wait for workers to finish
	close(jobs)
	wg.Wait()

	fmt.Println("All tasks completed. Closing MongoDB connection.")
}
