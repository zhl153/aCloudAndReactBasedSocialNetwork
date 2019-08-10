package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	//"github.com/olivere/elastic"
	elastic "gopkg.in/olivere/elastic.v6"
)

const (
	USER_INDEX = "user" // identify a node in elastic search
	USER_TYPE  = "user"
)

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Age      int64  `json:"age"`
	Gender   string `json:"gender"`
}

var mySigningKey = []byte("secret") // private key

func checkUser(username, password string) error { // check whether valid
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false)) // connect
	if err != nil {
		return err
	}
	// get user
	query := elastic.NewTermQuery("username", username) // make query

	searchResult, err := client.Search().
		Index(USER_INDEX). // search in USER_INDEX
		Query(query). // where username = ...
		Pretty(true).
		Do(context.Background())
	if err != nil {
		return err
	}
	// compare, if same log in
	var utyp User
	for _, item := range searchResult.Each(reflect.TypeOf(utyp)) { // 从searchResult导出所有结果并转换为User存入utyp
		if u, ok := item.(User); ok { // utyp中的每一个转换为User赋值给u
			if username == u.Username && password == u.Password {
				fmt.Printf("Login as %s\n", username)
				return nil
			}
		}
	}

	return errors.New("Wrong username or password") // 创建error消息
}

func addUser(user User) error { // sign up
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false)) // connect to es
	if err != nil {
		return err
	}

	// select * from users where username = ?
	query := elastic.NewTermQuery("username", user.Username) // username = ...
	// find user name in users
	searchResult, err := client.Search().
		Index(USER_INDEX).
		Query(query).
		Pretty(true).
		Do(context.Background()) // select * from users where username = ?
	if err != nil {
		return err
	}
	// if exist
	if searchResult.TotalHits() > 0 { // TotalHits is a convenience function to return the number of hits for a search result
		return errors.New("User already exists")
	}
	// add user
	_, err = client.Index().
		Index(USER_INDEX).
		Type(USER_TYPE).
		Id(user.Username).
		BodyJson(user).
		Refresh("wait_for"). // Wait for the changes made by the request to be made visible by a refresh before replying
		Do(context.Background())
	if err != nil {
		return err
	}

	fmt.Printf("User is added: %s\n", user.Username)
	return nil
}

func handlerLogin(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one login request")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method == "OPTIONS" {
		return
	}

	decoder := json.NewDecoder(r.Body) // get request body and get user
	var user User
	if err := decoder.Decode(&user); err != nil { // 判断request中是否存在username，若存在保存进user
		http.Error(w, "Cannot decode user data from client", http.StatusBadRequest)
		fmt.Printf("Cannot decode user data from client %v.\n", err)
		return
	}
	// check user if exist and match
	if err := checkUser(user.Username, user.Password); err != nil { // 若error非空，判断error类型
		if err.Error() == "Wrong username or password" {
			http.Error(w, "Wrong username or password", http.StatusUnauthorized)
		} else {
			http.Error(w, "Failed to read from ElasticSearch", http.StatusInternalServerError)
		}
		return
	}
	// Create a new token object, specifying signing method and the claims you would like it to contain.
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": user.Username,
		"exp":      time.Now().Add(time.Hour * 24).Unix(),
	})
	// send token to client
	// Sign and get the complete encoded token as a string using the secret(the private key)
	tokenString, err := token.SignedString(mySigningKey)
	if err != nil {
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		fmt.Printf("Failed to generate token %v.\n", err)
		return
	}

	w.Write([]byte(tokenString))
}

func handlerSignup(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one signup request")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method == "OPTIONS" {
		return
	}

	decoder := json.NewDecoder(r.Body) // 取出信息
	var user User
	if err := decoder.Decode(&user); err != nil { // 提取user
		http.Error(w, "Cannot decode user data from client", http.StatusBadRequest)
		fmt.Printf("Cannot decode user data from client %v.\n", err)
		return
	}
	// user name sanity check
	if user.Username == "" || user.Password == "" || !regexp.MustCompile(`^[a-z0-9_]+$`).MatchString(user.Username) {
		http.Error(w, "Invalid username or password", http.StatusBadRequest)
		fmt.Printf("Invalid username or password.\n")
		return
	}
	// add user
	if err := addUser(user); err != nil { // 非空判断错误类型
		if err.Error() == "User already exists" {
			http.Error(w, "User already exists", http.StatusBadRequest)
		} else {
			http.Error(w, "Failed to save to ElasticSearch", http.StatusInternalServerError)
		}
		return
	}

	w.Write([]byte("User added successfully."))
}
