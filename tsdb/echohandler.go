package tsdb

import (
	gormdb "github.com/heypkg/store/gorm"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"
)

func TSObjectHandler[T any](db any) echo.MiddlewareFunc {
	switch db2 := db.(type) {
	case *gorm.DB:
		return gormdb.TSObjectHandler[T](db2)
	}
	return nil
}
