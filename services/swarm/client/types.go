package client

import "time"

// Version represents the internal object version.
type Version struct {
	Index uint64 `json:",omitempty"`
}

// Meta is a base object inherited by most of the other once.
type Meta struct {
	Version   Version   `json:",omitempty"`
	CreatedAt time.Time `json:",omitempty"`
	UpdatedAt time.Time `json:",omitempty"`
}

type Service struct {
	ID string
	Meta
	Spec         ServiceSpec  `json:",omitempty"`
	PreviousSpec *ServiceSpec `json:",omitempty"`
	//	Endpoint     Endpoint      `json:",omitempty"`
	//	UpdateStatus *UpdateStatus `json:",omitempty"`
}

type ScaleSpec struct {
	// desired number of instances for the scaled object.
	Replicas int32 `json:"replicas,omitempty"`
}

// Annotations represents how to describe an object.
type Annotations struct {
	Name   string            `json:",omitempty"`
	Labels map[string]string `json:"Labels"`
}

// ServiceSpec represents the spec of a service.
type ServiceSpec struct {
	Annotations

	// TaskTemplate defines how the service should construct new tasks when
	// orchestrating this service.
	TaskTemplate TaskSpec    `json:",omitempty"`
	Mode         ServiceMode `json:",omitempty"`
}

type TaskSpec struct {
	ContainerSpec ContainerSpec `json:",omitempty"`
	ForceUpdate   uint64
}

type ContainerSpec struct {
	Image string `json:",omitempty"`
}
type ServiceMode struct {
	Replicated *ReplicatedService `json:",omitempty"`
	//Global     *GlobalService     `json:",omitempty"`
}
type ReplicatedService struct {
	Replicas *uint64 `json:",omitempty"`
}
