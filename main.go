package main

import (
	"context"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"os"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type Server struct {
	Clients map[string]*websocket.Conn
	Msg     string
	Reader  *kafka.Reader
	Writer  *kafka.Writer
}

func main() {

	var Server = Server{
		Clients: make(map[string]*websocket.Conn),
		Msg:     "",
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{"localhost:9093"},
			Topic:   "kafka-chat",
		}),
		Writer: &kafka.Writer{
			Addr:     kafka.TCP("localhost:9093"),
			Topic:    "kafka-chat",
			Balancer: &kafka.LeastBytes{},
		},
	}

	http.HandleFunc("/chat", Server.upgradeRequest)

	err := http.ListenAndServe(":"+os.Getenv("PORT"), nil)
	if err != nil {
		log.Fatalf("Failed to connect to server %s", err)
	}
}

func (srv Server) upgradeRequest(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade request %s\n", err)
		return
	}

	defer func(ws *websocket.Conn) {
		err := ws.Close()
		if err != nil {
			log.Printf("Failed to close the ws %s", err)
			return
		}
	}(ws)

	srv.Clients[r.URL.Query().Get("userID")] = ws
	senderID := r.URL.Query().Get("senderID")

	go func() {
		for {
			// read msg
			err := ws.ReadJSON(&srv.Msg)
			if err != nil {
				log.Printf("Error in reading the json msg %s", err)
				break
			}
			msg := kafka.Message{
				Key:   []byte(senderID),
				Value: []byte(srv.Msg),
			}

			err = srv.Writer.WriteMessages(context.Background(), msg)
			if err != nil {
				log.Println("")
			}
		}
	}()

	for {
		msg, rErr := srv.Reader.ReadMessage(context.Background())
		if rErr != nil {
			log.Println("Error in reading from kafka")
			return
		}

		if senderID == string(msg.Key) {
			conn, ok := srv.Clients[senderID]
			if ok {
				err := conn.WriteJSON(string(msg.Value))
				if err != nil {
					log.Printf("Error in writing msg %s", err)
					break
				}
			}
		}
	}
}
