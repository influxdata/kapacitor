package auth_test

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

var password = []byte("sorta long password")

func Benchmark_Bcrypt_Cost_Min(b *testing.B) {
	// NOTE: Min == 4
	for i := 0; i < b.N; i++ {
		bcrypt.GenerateFromPassword(password, bcrypt.MinCost)
	}
}
func Benchmark_Bcrypt_Cost_Default(b *testing.B) {
	// NOTE: Default == 10
	for i := 0; i < b.N; i++ {
		bcrypt.GenerateFromPassword(password, bcrypt.DefaultCost)
	}
}
func Benchmark_Bcrypt_Cost_11(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bcrypt.GenerateFromPassword(password, 11)
	}
}
func Benchmark_Bcrypt_Cost_12(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bcrypt.GenerateFromPassword(password, 12)
	}
}
func Benchmark_Bcrypt_Cost_13(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bcrypt.GenerateFromPassword(password, 13)
	}
}
func Benchmark_Bcrypt_Cost_14(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bcrypt.GenerateFromPassword(password, 14)
	}
}
