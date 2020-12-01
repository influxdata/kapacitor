package telegram

import (
	"net/url"

	"github.com/pkg/errors"
)

const DefaultTelegramURL = "https://api.telegram.org/bot"
const DefaultTelegramLinksPreviewDisable = false
const DefaultTelegramNotificationDisable = false

type Config struct {
	// Whether Telegram integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The Telegram Bot URL, should not need to be changed.
	URL string `toml:"url" override:"url"`
	// The Telegram Bot Token, can be obtained From @BotFather.
	Token string `toml:"token" override:"token,redact"`
	// The default channel, can be overridden per alert.
	ChatId string `toml:"chat-id" override:"chat-id"`
	// Send Markdown or HTML, if you want Telegram apps to show bold, italic, fixed-width text or inline URLs in your bot's message.
	ParseMode string `toml:"parse-mode" override:"parse-mode"`
	// Disables link previews for links in this message
	DisableWebPagePreview bool `toml:"disable-web-page-preview" override:"disable-web-page-preview"`
	// Sends the message silently. iOS users will not receive a notification, Android users will receive a notification with no sound.
	DisableNotification bool `toml:"disable-notification" override:"disable-notification"`
	// Whether all alerts should automatically post to Telegram
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`
}

func NewConfig() Config {
	return Config{
		URL:                   DefaultTelegramURL,
		DisableWebPagePreview: DefaultTelegramLinksPreviewDisable,
		DisableNotification:   DefaultTelegramNotificationDisable,
	}
}

func (c Config) Validate() error {
	if c.Enabled {
		if c.URL == "" {
			return errors.New("must specify url")
		}
		if c.Token == "" {
			return errors.New("must specify token")
		}
	}
	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid url %q", c.URL)
	}
	return nil
}
