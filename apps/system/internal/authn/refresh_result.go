package authn

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type refreshResult struct {
	AccessToken  string    `json:"accessToken"`
	RefreshToken string    `json:"refreshToken"`
	ExpiresAt    time.Time `json:"expiresAt"`
}

func (i *TokenIssuer) refreshResultCipher() (cipher.AEAD, error) {
	// Domain separation keeps encryption keys distinct from JWT signing keys.
	// All RPC instances derive the same key without an additional deployment secret.
	mac := hmac.New(sha256.New, []byte(i.config.AccessSecret))
	_, _ = mac.Write([]byte("dogx:refresh-result:v1:" + i.config.Issuer))
	block, err := aes.NewCipher(mac.Sum(nil))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (i *TokenIssuer) encryptRefreshResult(sessionID string, credentials *Credentials, expiresAt time.Time) (string, error) {
	aead, err := i.refreshResultCipher()
	if err != nil {
		return "", err
	}
	plain, err := json.Marshal(refreshResult{credentials.AccessToken, credentials.RefreshToken, expiresAt})
	if err != nil {
		return "", fmt.Errorf("encode refresh result: %w", err)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("generate refresh result nonce: %w", err)
	}
	// Session binding prevents moving a valid ciphertext to another session.
	sealed := aead.Seal(nonce, nonce, plain, []byte(sessionID))
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (i *TokenIssuer) decryptRefreshResult(sessionID, encoded string) (*Credentials, error) {
	aead, err := i.refreshResultCipher()
	if err != nil {
		return nil, err
	}
	sealed, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(sealed) < aead.NonceSize()+aead.Overhead() {
		return nil, errors.New("invalid encrypted refresh result")
	}
	plain, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], []byte(sessionID))
	if err != nil {
		return nil, errors.New("authenticate refresh result failed")
	}
	var result refreshResult
	if err := json.Unmarshal(plain, &result); err != nil {
		return nil, errors.New("decode refresh result failed")
	}
	expiresIn := int64(result.ExpiresAt.Sub(i.now()).Seconds())
	if result.AccessToken == "" || result.RefreshToken == "" || expiresIn <= 0 {
		return nil, errors.New("refresh result is invalid or expired")
	}
	return &Credentials{AccessToken: result.AccessToken, RefreshToken: result.RefreshToken, ExpiresIn: expiresIn}, nil
}
