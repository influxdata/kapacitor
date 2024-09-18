package kafka

import (
	"context"
	"github.com/IBM/sarama"
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

func NewRefreshingToken(source oauth2.TokenSource, extensions map[string]string) *RefreshingToken {
	return &RefreshingToken{
		source:     source,
		extensions: extensions,
	}
}

func (k *RefreshingToken) Token() (*sarama.AccessToken, error) {
	token, err := k.source.Token()
	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{
		Token:      token.AccessToken,
		Extensions: k.extensions,
	}, nil
}

func NewStaticToken(token string, extensions map[string]string) *StaticToken {
	return &StaticToken{
		token:      token,
		extensions: extensions,
	}
}

func (k *StaticToken) Token() (*sarama.AccessToken, error) {
	return &sarama.AccessToken{
		Token:      k.token,
		Extensions: k.extensions,
	}, nil
}
