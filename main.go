package main

import (
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"strings"
	"sync"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type MSG struct {
	ID  int    `json:"id"`
	Msg string `json:"message"`
}

type Server struct {
	Clients map[string]*websocket.Conn
	Msg     MSG
	Reader  *kafka.Reader
	Writer  *kafka.Writer
}

func main() {
	var Server = &Server{
		Clients: make(map[string]*websocket.Conn),
		Msg:     MSG{},
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{"localhost:9093"},
			Topic:   "kafka-chat",
			//Dialer:  dialer,
		}),
		Writer: &kafka.Writer{
			Addr:     kafka.TCP("localhost:9093"),
			Topic:    "kafka-chat",
			Balancer: &kafka.LeastBytes{},
		},
	}
	err := Server.Reader.SetOffset(kafka.LastOffset)
	if err != nil {
		log.Printf("Error in setting the offset of kafka %s", err)
		return
	}

	fmt.Println("Welcome to Chat")
	http.HandleFunc("/chat", Server.upgradeRequest)

	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("Failed to connect to server %s", err)
	}
}

func (srv *Server) upgradeRequest(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
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
	userID := r.URL.Query().Get("userId")
	//senderID := r.URL.Query().Get("senderId")
	srv.Clients[userID] = ws
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for {
			// read msg
			err := ws.ReadJSON(&srv.Msg)
			if err != nil {
				log.Printf("Error in reading the json msg %s", err)
				break
			}
			k := fmt.Sprintf("%s+%s", srv.Msg.ID, userID)
			msg := kafka.Message{
				Key:   []byte(k),
				Value: []byte(srv.Msg.Msg),
			}
			// write in kafka
			err = srv.Writer.WriteMessages(context.Background(), msg)
			if err != nil {
				log.Printf("Error in writing message %s\n", err)
				break
			}
		}
		wg.Done()
	}()
	go func() {
		for {
			// read from kafka
			msg, rErr := srv.Reader.ReadMessage(context.Background())
			if rErr != nil {
				log.Printf("Error in reading from kafka %s\n", rErr)
				return
			}
			//log.Printf("%v %v %v", string(msg.Key), string(msg.Value), senderID)
			id := string(msg.Key)
			i := strings.Index(id, "+")
			sID := id[0:i]
			uID := id[i+1:]
			//log.Printf(id)
			conn, ok := srv.Clients[sID]
			if ok {
				err := conn.WriteJSON(string(msg.Value))
				if err != nil {
					log.Printf("Error in writing msg %s", err)
					break
				}
				sConn, ok := srv.Clients[uID]
				if ok {
					err = sConn.WriteJSON(string(msg.Value))
					if err != nil {
						log.Printf("Error in writing msg to the sender %s", err)
						break
					}
				}
			}
		}
		wg.Done()
	}()
	wg.Wait()
}
