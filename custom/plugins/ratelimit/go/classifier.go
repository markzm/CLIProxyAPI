package main

import (
	"net/http"
	"strings"
)

// Classifier identifies client type from headers and request attributes.
type Classifier struct{}

// NewClassifier creates a new client type classifier.
func NewClassifier() *Classifier {
	return &Classifier{}
}

// Resolve returns the claimed client type from headers and request attributes.
// Headers are client claims, not authenticated identities; callers must map names that are not
// configured to the default bucket.
func (c *Classifier) Resolve(headers http.Header, sourceFormat string) string {
	if headers != nil {
		// 1. Explicit client header override.
		if ct := strings.TrimSpace(headers.Get("X-Client-Type")); ct != "" {
			return strings.ToLower(ct)
		}
		if ct := strings.TrimSpace(headers.Get("X-Caller-Type")); ct != "" {
			return strings.ToLower(ct)
		}

		// 2. User-Agent fingerprint.
		if clientType := matchUserAgent(headers.Get("User-Agent")); clientType != "" {
			return clientType
		}
	}

	// 3. Fallback based on the client protocol.
	switch strings.ToLower(strings.TrimSpace(sourceFormat)) {
	case "codex", "openai-response":
		return "codex"
	case "claude":
		return "claude-code"
	case "gemini":
		return "gemini"
	}
	return defaultTypeName
}

func matchUserAgent(ua string) string {
	if ua == "" {
		return ""
	}

	lowerUA := strings.ToLower(ua)

	// Codex official client family
	if strings.Contains(lowerUA, "codex desktop") ||
		strings.Contains(lowerUA, "codex_cli_rs") ||
		strings.Contains(lowerUA, "codex-cli") ||
		strings.Contains(lowerUA, "codex/") ||
		strings.Contains(lowerUA, "codex-tui") ||
		strings.Contains(lowerUA, "codex_exec") ||
		strings.Contains(lowerUA, "codex_vscode") {
		return "codex"
	}

	// Claude tool family
	if strings.Contains(lowerUA, "claude code") ||
		strings.Contains(lowerUA, "claude-cli") ||
		strings.Contains(lowerUA, "anthropic-sdk") {
		return "claude-code"
	}

	// Gemini tool family
	if strings.Contains(lowerUA, "antigravity") ||
		strings.Contains(lowerUA, "google-genai") ||
		strings.Contains(lowerUA, "gemini-cli") {
		return "gemini"
	}

	// IDE plugins
	if strings.Contains(lowerUA, "cursor") ||
		strings.Contains(lowerUA, "continue") ||
		strings.Contains(lowerUA, "cline") ||
		strings.Contains(lowerUA, "roo-code") ||
		strings.Contains(lowerUA, "chatbox") {
		return "ide-plugin"
	}

	// Automated scripts / crawlers
	if strings.Contains(lowerUA, "python-requests") ||
		strings.Contains(lowerUA, "aiohttp") ||
		strings.HasPrefix(lowerUA, "curl/") ||
		strings.Contains(lowerUA, "go-http-client") ||
		strings.Contains(lowerUA, "node-fetch") ||
		strings.Contains(lowerUA, "urllib") {
		return "script-bot"
	}

	return ""
}
