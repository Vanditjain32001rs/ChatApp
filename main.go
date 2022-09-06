package main

import (
	"github.com/gorilla/websocket"
	"log"
	"net/http"
)

var clients = make(map[string]*websocket.Conn)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type Message struct {
	Msg string `json:"message"`
}

func main() {

	http.HandleFunc("/chat", upgradeRequest)

	err := http.ListenAndServe(":8081", nil)
	if err != nil {
		log.Fatalf("Failed to connect to server %s", err)
	}
}

func upgradeRequest(w http.ResponseWriter, r *http.Request) {
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

	clients[r.URL.Query().Get("userID")] = ws

	var msg Message

	for {
		// read msg
		err := ws.ReadJSON(&msg.Msg)
		if err != nil {
			log.Printf("Error in reading the json msg %s", err)
			break
		}

		conn, ok := clients[r.URL.Query().Get("senderID")]
		if ok {
			err := conn.WriteJSON(msg.Msg)
			if err != nil {
				log.Printf("Error in writing msg %s", err)
				break
			}
		}
	}
}
