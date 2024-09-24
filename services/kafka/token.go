package kafka

import (
	"context"

	kafka "github.com/IBM/sarama"
	"golang.org/x/oauth2"
)

type RefreshingToken struct {
	source     oauth2.TokenSource
	cancel     context.CancelFunc
	extensions map[string]string
}

type StaticToken struct {
	token      string
	extensions map[string]string
}

func NewRefreshingToken(source oauth2.TokenSource, cancel context.CancelFunc, extensions map[string]string) *RefreshingToken {
	return &RefreshingToken{
		source:     source,
		cancel:     cancel,
		extensions: extensions,
	}
}

func (k *RefreshingToken) Token() (*kafka.AccessToken, error) {
	token, err := k.source.Token()
	if err != nil {
		return nil, err
	}
	return &kafka.AccessToken{
		Token:      token.AccessToken,
		Extensions: k.extensions,
	}, nil
}

func (k *RefreshingToken) Close() {
	// canceling the token refresh
	k.cancel()
}

func NewStaticToken(token string, extensions map[string]string) *StaticToken {
	return &StaticToken{
		token:      token,
		extensions: extensions,
	}
}

func (k *StaticToken) Token() (*kafka.AccessToken, error) {
	return &kafka.AccessToken{
		Token:      k.token,
		Extensions: k.extensions,
	}, nil
}
