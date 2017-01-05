/*
  Alert provides an implementation of the HTTP API for managing alert topics, handlers and events.

  Responsibilities of this package include:

  * Providing and HTTP API for the management of handlers
  * Storing handler definitions
  * Providing implementations of several simple handlers
  * Mapping external handler implementations to handler definitions

  The last item needs some more clarification.
  Handlers can be implemented in external packages.
  In order for the HTTP API to consume those implementations
  a mapping needs to be defined from the HandlerAction definition to
  the expected configuration of the handler implementation.
  This package provides that mapping.

*/
package alert
