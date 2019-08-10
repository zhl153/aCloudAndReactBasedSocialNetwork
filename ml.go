package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"golang.org/x/oauth2/google"
)

type Prediction struct { // each response's structure
	Prediction int       `json:"prediction"` 
	Key        string    `json:"key"`// corresponding key in the response body
	Scores     []float64 `json:"scores"`
}

type MLResponseBody struct { // the item of key "prediction"
	Predictions []Prediction `json:"predictions"` // include an array of Predictions(multiple inputs)
}

type ImageBytes struct { // structure of ImageBytes
	B64 []byte `json:"b64"`
}
type Instance struct { // structure of "Instance"(input)
	ImageBytes ImageBytes `json:"image_bytes"`
	Key        string     `json:"key"`
}

type MLRequestBody struct { // request structure
	Instances []Instance `json:"instances"`
}

const (
	// Replace this project ID and model name with your configuration.
	PROJECT = "true-source-241502" // id
	MODEL   = "my_model"           // model name
	URL     = "https://ml.googleapis.com/v1/projects/" + PROJECT + "/models/" + MODEL + ":predict"
	SCOPE   = "https://www.googleapis.com/auth/cloud-platform" // api scope
)

// Annotate an image file based on ml model, return score and error if exists. Provide face recognition
func annotate(r io.Reader) (float64, error) { // take reader, return possibility in float
	// func ReadAll(r io.Reader) ([]byte, error)
	// ReadAll reads from r until an error or EOF and returns the data it read.
	buf, err := ioutil.ReadAll(r) // read input from Reader
	if err != nil {
		fmt.Printf("Cannot read image data %v\n", err)
		return 0.0, err
	}
	// DefaultClient returns an HTTP Client that uses the DefaultTokenSource to obtain authentication credentials
	client, err := google.DefaultClient(context.Background(), SCOPE) // use default client constructor, include token, take new context
	if err != nil {
		fmt.Printf("Failed to create HTTP client %v\n", err)
		return 0.0, err
	}

	// Construct a ML request
	requestBody := &MLRequestBody{ // request body, constructor
		Instances: []Instance{ // Instance constructor
			{
				ImageBytes: ImageBytes{ // type of image
					B64: buf, // Base64 Image Encoder
				},
				Key: "1", // one image at a time
			},
		},
	}
	jsonRequestBody, err := json.Marshal(requestBody) // change request body to json format, encoding
	if err != nil {
		fmt.Printf("Failed to create ML request body %v\n", err)
		return 0.0, err
	}

	request, err := http.NewRequest("POST", URL, strings.NewReader(string(jsonRequestBody))) // create http request, method, url, body(type reader)

	response, err := client.Do(request) // get response
	if err != nil {
		fmt.Printf("Failed to send ML request %v\n", err)
		return 0.0, err
	}
	// take out result
	jsonResponseBody, err := ioutil.ReadAll(response.Body) // use ioutil.ReadAll() to read response
	if err != nil {
		fmt.Printf("Failed to get ML response body %v\n", err)
		return 0.0, err
	}
	// sanity check
	if len(jsonResponseBody) == 0 {
		fmt.Println("Empty prediction response body")
		return 0.0, errors.New("Empty prediction response body")
	}

	var responseBody MLResponseBody
	// Unmarshal parses the JSON-encoded data and stores the result in the value pointed to by v
	if err := json.Unmarshal(jsonResponseBody, &responseBody); err != nil { // json to go struct
		fmt.Printf("Failed to decode ML response %v\n", err)
		return 0.0, err
	}
	// sanity check
	if len(responseBody.Predictions) == 0 {
		fmt.Println("Empty prediction result")
		return 0.0, errors.New("Empty prediction result")
	}

	results := responseBody.Predictions[0]
	fmt.Printf("Received a prediction result %f\n", results.Scores[0])
	return results.Scores[0], nil
}
