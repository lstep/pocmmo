package main

import (
	"context"
	//"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
)

// Structure des données d'un joueur [cite: 8]
type Player struct {
	ID string  `json:"id"`
	X  float64 `json:"x"`
	Y  float64 `json:"y"`
}

// Structure générique des messages réseau
type Message struct {
	Type    string            `json:"type"`
	ID      string            `json:"id,omitempty"`
	X       float64           `json:"x,omitempty"`
	Y       float64           `json:"y,omitempty"`
	Players map[string]Player `json:"players,omitempty"`
}

// Serveur d'état en mémoire vive [cite: 8]
type Server struct {
	clients map[string]*websocket.Conn
	players map[string]Player
	mu      sync.RWMutex
}

func NewServer() *Server {
	return &Server{
		clients: make(map[string]*websocket.Conn),
		players: make(map[string]Player),
	}
}

func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	// Upgrade de la connexion HTTP en WebSocket [cite: 8]
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // Autorise les connexions locales en dev
	})
	if err != nil {
		log.Println("Échec de l'Upgrade WebSocket:", err)
		return
	}
	defer conn.CloseNow()

	// Génération d'un ID de session unique pour l'employé [cite: 9]
	clientID := uuid.New().String()

	s.mu.Lock()
	s.clients[clientID] = conn
	// Position d'apparition par défaut au milieu de l'écran (800x600) [cite: 8]
	s.players[clientID] = Player{ID: clientID, X: 400, Y: 300}
	
	// Préparation du message d'initialisation pour le nouveau connecté
	initMsg := Message{
		Type:    "init",
		ID:      clientID,
		Players: s.players,
	}
	s.mu.Unlock()

	ctx := r.Context()

	// Envoyer l'état initial complet au joueur qui vient de se connecter
	if err := wsjson.Write(ctx, conn, initMsg); err != nil {
		s.cleanup(clientID)
		return
	}

	// Signaler la présence du nouveau joueur à tous les autres connectés
	s.broadcastExcept(clientID, Message{
		Type: "join",
		ID:   clientID,
		X:    400,
		Y:    300,
	})

	log.Printf("Collaborateur connecté : %s (Total: %d)\n", clientID, len(s.clients))

	// Boucle d'écoute de l'état du joueur [cite: 8]
	for {
		var incoming Message
		err := wsjson.Read(ctx, conn, &incoming)
		if err != nil {
			log.Printf("Déconnexion du collaborateur : %s\n", clientID)
			s.cleanup(clientID)
			break
		}

		if incoming.Type == "move" {
			s.mu.Lock()
			if p, exists := s.players[clientID]; exists {
				p.X = incoming.X
				p.Y = incoming.Y
				s.players[clientID] = p
			}
			s.mu.Unlock()

			// Transmettre le déplacement à tous les autres utilisateurs connectés [cite: 8]
			s.broadcastExcept(clientID, Message{
				Type: "update",
				ID:   clientID,
				X:    incoming.X,
				Y:    incoming.Y,
			})
		}
	}
}

// Nettoyage de la connexion et retrait de l'avatar
func (s *Server) cleanup(id string) {
	s.mu.Lock()
	delete(s.clients, id)
	delete(s.players, id)
	s.mu.Unlock()

	s.broadcastAll(Message{
		Type: "leave",
		ID:   id,
	})
}

// Diffuse à tout le monde sauf à l'expéditeur [cite: 8, 10]
func (s *Server) broadcastExcept(exceptID string, msg Message) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, conn := range s.clients {
		if id != exceptID {
			_ = wsjson.Write(context.Background(), conn, msg)
		}
	}
}

// Diffuse à l'intégralité des connectés [cite: 8, 10]
func (s *Server) broadcastAll(msg Message) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, conn := range s.clients {
		_ = wsjson.Write(context.Background(), conn, msg)
	}
}

func main() {
	server := NewServer()

	// Handler pour l'Upgrade WebSocket [cite: 8]
	http.HandleFunc("/ws", server.HandleWS)

	// Serveur de fichiers statiques pour héberger notre client web [cite: 7]
	http.Handle("/", http.FileServer(http.Dir("./public")))

	log.Println("PoC démarré avec succès sur http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Erreur ListenAndServe:", err)
	}
}
