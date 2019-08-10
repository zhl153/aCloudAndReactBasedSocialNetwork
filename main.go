package main

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	//"github.com/olivere/elastic" // <- Add this
	"github.com/pborman/uuid" // <- Add this
	elastic "gopkg.in/olivere/elastic.v6" // https://godoc.org/github.com/olivere/elastic#example-NewClient--ManyOptions
	"io"
	"log"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	// Use JWT to Protect Post and Search Endpoints
	jwtmiddleware "github.com/auth0/go-jwt-middleware" // https://godoc.org/github.com/auth0/go-jwt-middleware
	jwt "github.com/dgrijalva/jwt-go"
)

const (
	POST_INDEX          = "post"
	POST_TYPE           = "post"
	DISTANCE            = "200km"
	BUCKET_NAME         = "my-post-images"
	BIGTABLE_PROJECT_ID = "true-source-241502"
	BT_INSTANCE         = "around-post"
	ES_URL              = "http://10.128.0.2:9200" // change this every time when start!!!
	// API_PREFIX       = "/api/v1" // version if any
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	// `json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
	Type     string   `json:"type"` // file type
	Face     float64  `json:"face"` // predict result
}

var ( // upload file type, type -> image or video
	mediaTypes = map[string]string{
		".jpeg": "image",
		".jpg":  "image",
		".gif":  "image",
		".png":  "image",
		".mov":  "video",
		".mp4":  "video",
		".avi":  "video",
		".flv":  "video",
		".wmv":  "video",
	}
)

func main() {
	fmt.Println("started-service")

	createIndexIfNotExist() // create elastic search
	// token操作jwtMiddleware
	jwtMiddleware := jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) { // take a token return a sign in key. The function that will return the Key to validate the JWT.
			return []byte(mySigningKey), nil
		},
		SigningMethod: jwt.SigningMethodHS256, // decryption algorithm
	}) // token验证集成

	r := mux.NewRouter() // gorilla/mux library, https://www.gorillatoolkit.org/pkg/mux, 
	// .Handle() registers a new route with a matcher for the URL path, Router implements the http.Handler interface, so it can be registered to serve requests
	//.Methods() match HTTP methods
	r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST", "OPTIONS")    // handle with jwt middleware
	r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET", "OPTIONS") // hanle with ...
	r.Handle("/cluster", jwtMiddleware.Handler(http.HandlerFunc(handlerCluster))).Methods("GET", "OPTIONS")
	r.Handle("/signup", http.HandlerFunc(handlerSignup)).Methods("POST", "OPTIONS")
	r.Handle("/login", http.HandlerFunc(handlerLogin)).Methods("POST", "OPTIONS")

	http.Handle("/", r)

	// func HandleFunc(pattern string, handler func(ResponseWriter, *Request))
	// HandleFunc registers the handler function for the given pattern in the DefaultServeMux.
	// http.HandleFunc("/post", handlerPost)
	// http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")

	w.Header().Set("Content-Type", "application/json") // return type
	w.Header().Set("Access-Control-Allow-Origin", "*") // available to all clients
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization") // 预检请求

	user := r.Context().Value("user") // get user from token
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"] // decrypt and get user name

	if r.Method == "OPTIONS" {
		return
	}

	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

	p := &Post{
		User:    username.(string),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	} // post object

	id := uuid.New() // returns a new random (version 4) UUID as a string
	file, _, err := r.FormFile("image") // get file
	if err != nil {
		http.Error(w, "Image is not available", http.StatusBadRequest)
		fmt.Printf("Image is not available %v.\n", err)
		return
	}
	attrs, err := saveToGCS(file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "Failed to save image to GCS", http.StatusInternalServerError)
		fmt.Printf("Failed to save image to GCS %v.\n", err)
		return
	}
	p.Url = attrs.MediaLink // return file url on GCS

	file, header, _ := r.FormFile("image")  // read image
	// FormFile returns the first file for the provided form key.
	suffix := filepath.Ext(header.Filename) // file type
	// Ext returns the file name extension used by path
	if t, ok := mediaTypes[suffix]; ok {
		p.Type = t // videos/images
	} else {
		p.Type = "unknown"
	}
	if suffix == ".jpeg" { // default type is .jpeg, else 0.0
		if score, err := annotate(file); err != nil {
			http.Error(w, "Failed to annotate the image", http.StatusInternalServerError)
			fmt.Printf("Failed to annotate the image %v\n", err)
			return
		} else {
			p.Face = score
		}
	} else {
		p.Face = 0.0
	}

	err = saveToES(p, id)
	if err != nil {
		http.Error(w, "Failed to save post to ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to save post to ElasticSearch %v.\n", err)
		return
	}
	fmt.Printf("Saved one post to ElasticSearch: %s\n", p.Message)

	//err = saveToBigTable(p, id) // to use big table and big query
	//if err != nil {
	//	http.Error(w, "Failed to save post to BigTable", http.StatusInternalServerError)
	//	fmt.Printf("Failed to save post to BigTable %v.\n", err)
	//	return
	//}

	/*
		// Parse from body of request to get a json object.
		fmt.Println("Received one post request")

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

		if r.Method == "OPTIONS" {
			return
		}

		decoder := json.NewDecoder(r.Body) // read from input stream
		var p Post
		if err := decoder.Decode(&p); err != nil { // turn the inputs to go object into 'p': key, value pairs.
			http.Error(w, "Cannot decode post data from client", http.StatusBadRequest)
			fmt.Printf("Cannot decode post data from client %v.\n", err)
			return
		}
		id := uuid.New()
		err := saveToES(&p, id)
		if err != nil {
			http.Error(w, "Failed to save post to ElasticSearch", http.StatusInternalServerError)
			fmt.Printf("Failed to save post to ElasticSearch %v.\n", err)
			return
		}
		fmt.Printf("Saved one post to ElasticSearch: %s", p.Message)*/
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	if r.Method == "OPTIONS" {
		return
	}

	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	query := elastic.NewGeoDistanceQuery("location") // construct query
	query = query.Distance(ran).Lat(lat).Lon(lon)

	posts, err := readFromES(query)
	if err != nil {
		http.Error(w, "Failed to read post from ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to read post from ElasticSearch %v.\n", err)
		return
	}

	js, err := json.Marshal(posts) // Convert the go object to a string
	if err != nil {
		http.Error(w, "Failed to parse posts into JSON format", http.StatusInternalServerError)
		fmt.Printf("Failed to parse posts into JSON format %v.\n", err)
		return
	}

	w.Write(js)

	/*
		fmt.Println("range is ", ran)

		// Return a fake post
		p := &Post{
			User:    "1111",
			Message: "一生必去的100个地方",
			Location: Location{
				Lat: lat,
				Lon: lon,
			},
		}

		js, err := json.Marshal(p) // turn a go object into a json formated object, match relative keys
		if err != nil {
			panic(err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(js)*/
}

func handlerCluster(w http.ResponseWriter, r *http.Request) { // similar to handler search
	fmt.Println("Received one cluster request")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	if r.Method == "OPTIONS" {
		return
	}
	// func NewRangeQuery(name string) *RangeQuery, creates and initializes a new RangeQuery
	term := r.URL.Query().Get("term")
	query := elastic.NewRangeQuery(term).Gte(0.97) // predict threshold, Gte() indicates a greater-than-or-equal value for the from part. 

	posts, err := readFromES(query)
	if err != nil {
		http.Error(w, "Failed to read post from ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to read post from ElasticSearch %v.\n", err)
		return
	}

	js, err := json.Marshal(posts)
	if err != nil {
		http.Error(w, "Failed to parse post object", http.StatusInternalServerError)
		fmt.Printf("Failed to parse post object %v\n", err)
		return
	}

	w.Write(js)
}

func createIndexIfNotExist() { // APIs are from "github.com/olivere/elastic", doc "https://godoc.org/github.com/olivere/elastic#example-NewClient--ManyOptions"
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))  // connect to ES
	// create a new client to work with ES, SetSniff = distributed storage
	if err != nil {
		panic(err)
	}

	exists, err := client.IndexExists(POST_INDEX).Do(context.Background())  // checks if a given index already exists
	if err != nil {
		panic(err)
	}

	if !exists { // create if not exist
		mapping := `{
            "mappings": {
                "post": {
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                    }
                }
            }
        }` // pattern
		_, err = client.CreateIndex(POST_INDEX).Body(mapping).Do(context.Background()) // CreateIndex returns a service to create a new index.
		if err != nil {
			panic(err)
		}
	}
	// check if USER index exist
	exists, err = client.IndexExists(USER_INDEX).Do(context.Background()) // user database
	if err != nil {
		panic(err)
	}
	// if not, create
	if !exists {
		_, err = client.CreateIndex(USER_INDEX).Do(context.Background()) // create user
		if err != nil {
			panic(err)
		}
	}
}

// Save a post to ElasticSearch
func saveToES(post *Post, id string) error {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false)) // connect to ES
	if err != nil {
		return err
	}

	_, err = client.Index().
		Index(POST_INDEX). // save to POST
		Type(POST_TYPE).
		Id(id).
		BodyJson(post). // item body
		Refresh("wait_for").
		Do(context.Background()) // run
	if err != nil {
		return err
	}

	fmt.Printf("Post is saved to index: %s\n", post.Message)
	return nil
}

func saveToBigTable(p *Post, id string) error {

	ctx := context.Background()
	bt_client, err := bigtable.NewClient(ctx, BIGTABLE_PROJECT_ID, BT_INSTANCE) // connect to big table
	if err != nil {
		return err
	}

	tbl := bt_client.Open("post")              // open a table
	mut := bigtable.NewMutation()              // create record
	t := bigtable.Now()                        // time stamp
	// Set(family, column string, ts Timestamp, value []byte)
	// Set sets a value in a specified column, with the given timestamp.
	mut.Set("post", "user", t, []byte(p.User)) // write message
	mut.Set("post", "message", t, []byte(p.Message))
	mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

	err = tbl.Apply(ctx, id, mut) // add to bigtable, apply mutates a row atomically
	if err != nil {
		return err
	}
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
	return nil

}

// distance query
func readFromES(query elastic.Query) ([]Post, error) {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false)) // connect
	if err != nil {
		return nil, err
	}

	// 	query := elastic.NewGeoDistanceQuery("location") // GeoDistanceQuery filters documents that include only hits that exists within a specific distance from a geo point.
	// 	query = query.Distance(ran).Lat(lat).Lon(lon) // set query 
	// // use query as an input afterwards
	searchResult, err := client.Search().
		Index(POST_INDEX).
		Query(query).
		Pretty(true). // format
		Do(context.Background()) // do search
	if err != nil {
		return nil, err
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	var ptyp Post
	var posts []Post
	for _, item := range searchResult.Each(reflect.TypeOf(ptyp)) { // reflect allows a program to manipulate objects with arbitrary types
		if p, ok := item.(Post); ok { // A type assertion provides access to an interface value's underlying concrete value. This statement asserts that the interface value i holds the concrete type T and assigns the underlying T value to the variable t.
			posts = append(posts, p) // casting -> add
		}
	}

	return posts, nil
}

func saveToGCS(r io.Reader, bucketName, objectName string) (*storage.ObjectAttrs, error) {
	ctx := context.Background() // more on context: https://blog.golang.org/context

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName) // make a bucket handle
	if _, err := bucket.Attrs(ctx); err != nil { // retrieve a bucket's attributes
		return nil, err
	}

	object := bucket.Object(objectName) // refer to objects using a handle, 
	wc := object.NewWriter(ctx)  // wc implements io.Writer
	if _, err = io.Copy(wc, r); err != nil { // write
		return nil, err
	}
	if err := wc.Close(); err != nil {
		return nil, err
	}

	if err = object.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil { // Access Control Lists, "allUsers", "READER" 
		return nil, err
	}

	attrs, err := object.Attrs(ctx) // return attributes
	if err != nil {
		return nil, err
	}

	fmt.Printf("Image is saved to GCS: %s\n", attrs.MediaLink)
	return attrs, nil
}
