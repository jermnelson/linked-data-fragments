/*
Package server implements a REST api and HTML management for the Linked
Data Fragments Server project. Inspired by Doug Black's blog post
http://dougblack.io/words/a-restful-micro-framework-in-go.html

Author = Jeremy Nelson
License = GPL Affero v3
 */
package main

import (
    "html/template"
    "fmt"
    "net/http"
    "net/url"
    "github.com/dougblack/sleepy"
)


type Triple struct {

}

func (triple Triple) Get(values url.Values, headers http.Header) (int, interface{}, http.Header) {
  data := map[string][]string{"s": "subject", "p": "predicate", "o": "object"}
  return 200, data, http.Header{"Content-type": {"application/json"}}
  
}

func main() {
    triple := new(Triple)
    port := 18150
    api := sleepy.NewAPI()
    api.AddResource(triple, "/triples")
    api.Start(port)

}

func main_old() {
    port := 18150
    fmt.Printf("Running on port %v\n", port)
    http.HandleFunc("/", handler)
    http.Handle("/images/", http.StripPrefix("/images/", http.FileServer(http.Dir("images"))))
    http.ListenAndServe(fmt.Sprintf(":%v", port), nil)
}
