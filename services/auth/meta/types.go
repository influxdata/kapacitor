package meta

import (
	"fmt"
)

// Error represent an error message sent in a response body from a meta
// node.
type Error struct {
	Message string `json:"error"`
	Code    int    `json:"-"`
}

// NewError returns a new Error value.
func NewError(msg string, code int) Error {
	return Error{Message: msg, Code: code}
}

func (e Error) Error() string {
	return e.Message
}

// User holds information about a user's credentials and permissions.
type User struct {
	Name        string                  `json:"name"`
	Hash        string                  `json:"hash"`
	Permissions map[string][]Permission `json:"permissions,omitempty"`
}

// Permission type
// NOTE: Keep this type up-to-date with the corresponding type in meta/data.go
// The type cannot be imported as it creates an import cycle.
type Permission int

const (
	NoPermissions                   Permission = 0
	ViewAdminPermission             Permission = 1
	ViewChronografPermission        Permission = 2
	CreateDatabasePermission        Permission = 3
	CreateUserAndRolePermission     Permission = 4
	AddRemoveNodePermission         Permission = 5
	DropDatabasePermission          Permission = 6
	DropDataPermission              Permission = 7
	ReadDataPermission              Permission = 8
	WriteDataPermission             Permission = 9
	RebalancePermission             Permission = 10
	ManageShardPermission           Permission = 11
	ManageContinuousQueryPermission Permission = 12
	ManageQueryPermission           Permission = 13
	ManageSubscriptionPermission    Permission = 14
	MonitorPermission               Permission = 15
	CopyShardPermission             Permission = 16
	KapacitorAPIPermission          Permission = 17
	KapacitorConfigAPIPermission    Permission = 18
)

// MarshalText converts a Permission into a textual represenation.
func (p Permission) MarshalText() ([]byte, error) {
	switch p {
	case NoPermissions:
		return []byte("NoPermissions"), nil
	case ViewAdminPermission:
		return []byte("ViewAdmin"), nil
	case ViewChronografPermission:
		return []byte("ViewChronograf"), nil
	case CreateDatabasePermission:
		return []byte("CreateDatabase"), nil
	case CreateUserAndRolePermission:
		return []byte("CreateUserAndRole"), nil
	case AddRemoveNodePermission:
		return []byte("AddRemoveNode"), nil
	case DropDatabasePermission:
		return []byte("DropDatabase"), nil
	case DropDataPermission:
		return []byte("DropData"), nil
	case ReadDataPermission:
		return []byte("ReadData"), nil
	case WriteDataPermission:
		return []byte("WriteData"), nil
	case RebalancePermission:
		return []byte("Rebalance"), nil
	case ManageShardPermission:
		return []byte("ManageShard"), nil
	case ManageContinuousQueryPermission:
		return []byte("ManageContinuousQuery"), nil
	case ManageQueryPermission:
		return []byte("ManageQuery"), nil
	case ManageSubscriptionPermission:
		return []byte("ManageSubscription"), nil
	case MonitorPermission:
		return []byte("Monitor"), nil
	case CopyShardPermission:
		return []byte("CopyShard"), nil
	case KapacitorAPIPermission:
		return []byte("KapacitorAPI"), nil
	case KapacitorConfigAPIPermission:
		return []byte("KapacitorConfigAPI"), nil
	default:
		return nil, fmt.Errorf("invalid permission: %d", p)
	}
}

// UnmarshalText sets the permission based on the textual represenation.
func (p *Permission) UnmarshalText(t []byte) error {
	switch s := string(t); s {
	case "NoPermissions":
		*p = NoPermissions
	case "ViewAdmin":
		*p = ViewAdminPermission
	case "ViewChronograf":
		*p = ViewChronografPermission
	case "CreateDatabase":
		*p = CreateDatabasePermission
	case "CreateUserAndRole":
		*p = CreateUserAndRolePermission
	case "AddRemoveNode":
		*p = AddRemoveNodePermission
	case "DropDatabase":
		*p = DropDatabasePermission
	case "DropData":
		*p = DropDataPermission
	case "ReadData":
		*p = ReadDataPermission
	case "9", "WriteData":
		*p = WriteDataPermission
	case "Rebalance":
		*p = RebalancePermission
	case "ManageShard":
		*p = ManageShardPermission
	case "ManageContinuousQuery":
		*p = ManageContinuousQueryPermission
	case "ManageQuery":
		*p = ManageQueryPermission
	case "ManageSubscription":
		*p = ManageSubscriptionPermission
	case "Monitor":
		*p = MonitorPermission
	case "CopyShard":
		*p = CopyShardPermission
	case "KapacitorAPI":
		*p = KapacitorAPIPermission
	case "KapacitorConfigAPI":
		*p = KapacitorConfigAPIPermission
	default:
		return fmt.Errorf("invalid permission: %s", s)
	}
	return nil
}
