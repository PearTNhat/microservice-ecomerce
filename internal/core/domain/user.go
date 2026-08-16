package domain

import "time"

const (
	RoleCustomer   = "CUSTOMER"
	RoleTechnician = "TECHNICIAN"
	RoleAdmin      = "ADMIN"
)

type User struct {
	ID        uint      `json:"id" gorm:"primaryKey"`
	FirstName string    `json:"first_name"`
	LastName  string    `json:"last_name"`
	Email     string    `json:"email" gorm:"uniqueIndex;not null"`
	Password  string    `json:"password" gorm:"not null"`
	Code      int       `json:"code"`
	Expiry    time.Time `json:"expiry"`
	Verified  bool      `json:"verified" gorm:"default:false"`
	UserType  string    `json:"user_type" gorm:"default:'CUSTOMER'"`
	CraateAt  time.Time `json:"create_at" gorm:"default:current_timestamp"`
	UpdatedAt time.Time `json:"update_at" gorm:"default:current_timestamp"`
}

type UserRepository interface {
	CreateUser(user *User) error
	FindUserByEmail(email string) (*User, error)
	FindUserById(id uint) (*User, error)
	UpdateUser(user *User) error
}
