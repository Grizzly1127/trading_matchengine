package authserver

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
)

// Server 轻量服务 JWT 签发（dev/staging；生产可换外部 IdP）。
type Server struct {
	Secret  []byte
	Config  Config
	Clients map[string]ClientConfig
}

type tokenRequest struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

func New(cfg Config) (*Server, error) {
	secret, err := auth.ReadSecretFile(cfg.HS256SecretFile)
	if err != nil {
		return nil, err
	}
	clients := make(map[string]ClientConfig, len(cfg.Clients))
	for _, c := range cfg.Clients {
		clients[c.ClientID] = c
	}
	return &Server{Secret: secret, Config: cfg, Clients: clients}, nil
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/token", s.handleToken)
	return mux
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	var req tokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeTokenError(w, http.StatusBadRequest, "invalid json")
		return
	}
	client, ok := s.Clients[strings.TrimSpace(req.ClientID)]
	if !ok || client.ClientSecret != req.ClientSecret {
		writeTokenError(w, http.StatusUnauthorized, "invalid client credentials")
		return
	}
	ttl := time.Duration(s.Config.TokenTTLSeconds) * time.Second
	token, err := auth.SignHS256(s.Secret, s.Config.Issuer, client.ClientID, s.Config.Audience, client.Scopes, ttl)
	if err != nil {
		writeTokenError(w, http.StatusInternalServerError, "sign token failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   s.Config.TokenTTLSeconds,
	})
}

func writeTokenError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
