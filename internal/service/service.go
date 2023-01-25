package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/aririfani/message-service/internal/collector"
	"github.com/aririfani/message-service/internal/message"
	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
)

func Run(brokers []string, stream goka.Stream) {
	view, err := goka.NewView(brokers, collector.Table, new(collector.MessageListCodec))
	if err != nil {
		panic(err)
	}

	go view.Run(context.Background())

	emitter, err := goka.NewEmitter(brokers, stream, new(message.MessageCodec))
	if err != nil {
		panic(err)
	}

	defer emitter.Finish()

	fmt.Println("view", view)

	router := mux.NewRouter()
	router.HandleFunc("/{user}/send", send(emitter, stream)).Methods("POST")
	router.HandleFunc("/{user}/feed", feed(view)).Methods("GET")

	log.Printf("Listen port 8000")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func send(emitter *goka.Emitter, stream goka.Stream) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var m message.Message

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		err = json.Unmarshal(b, &m)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
		}

		m.From = mux.Vars(r)["user"]

		if stream == message.ReceivedStream {
			err = emitter.EmitSync(m.To, &m)
		} else {
			err = emitter.EmitSync(m.From, &m)
		}

		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		log.Printf("Sent message:\n %v\n", m)
		fmt.Println(w, "Sent message:\n %v\n", m)
	}
}

func feed(view *goka.View) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		user := mux.Vars(r)["user"]
		val, _ := view.Get(user)
		fmt.Println("view topic", val)
		if val == nil {
			fmt.Fprintf(w, "%s not found!", user)
			return
		}

		messages := val.([]message.Message)
		fmt.Fprintf(w, "Latest message for %s\n", user)
		for i, m := range messages {
			fmt.Fprintf(w, "%d %10s: %v\n", i, m.From, m.Content)
		}
	}
}
