package webexteams

const (
	// InvalidTokenErr is for missing, expired, or invalid access tokens
	InvalidTokenErr = "webexteams: invalid access token"
	// InvalidURLErr is for a bad URL
	InvalidURLErr = "webexteams: invalid url"
	// ServiceUnavailableErr is for when an alert is triggered but the Webex Teams Config has not been enabled
	ServiceUnavailableErr = "webexteams: service unavailable"
	// MissingDestinationErr is for a bad configuration, missing roomId, personId, or toPersonEmail
	MissingDestinationErr = "webexteams: a destination is required to send a webex teams message (roomId, personId, toPersonEmail)"
)
