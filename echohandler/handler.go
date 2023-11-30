package echohandler

import (
	gormdb "github.com/heypkg/store/gorm"
	"github.com/heypkg/store/search"
	"github.com/heypkg/store/utils"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

func ListObjects[T any](db any, c echo.Context, selectNames []string, handleFuncs map[string]search.SearchDataHandleFunc) ([]T, int64, error) {
	switch db2 := db.(type) {
	case *gorm.DB:
		return gormdb.ListObjects[T](db2, c, selectNames, handleFuncs)
	}
	return nil, 0, errors.New("invalid db")
}

func ListDeletedObjects[T any](db any, c echo.Context, selectNames []string, handleFuncs map[string]search.SearchDataHandleFunc) ([]T, int64, error) {
	switch db2 := db.(type) {
	case *gorm.DB:
		return gormdb.ListDeletedObjects[T](db2, c, selectNames, handleFuncs)
	}
	return nil, 0, errors.New("invalid db")
}

func GetObjectFromEchoContext[T any](c echo.Context) *T {
	var obj T
	key := utils.GetRawTypeName(obj)
	if v := c.Get(key); v != nil {
		if out, ok := v.(*T); ok {
			return out
		}
	}
	return nil
}

func ObjectHandler[T any](db any) echo.MiddlewareFunc {
	switch db2 := db.(type) {
	case *gorm.DB:
		return gormdb.ObjectHandler[T](db2)
	}
	return nil
}

func DeletedObjectHandler[T any](db any) echo.MiddlewareFunc {
	switch db2 := db.(type) {
	case *gorm.DB:
		return gormdb.DeletedObjectHandler[T](db2)
	}
	return nil
}

func TSObjectHandler[T any](db any) echo.MiddlewareFunc {
	switch db2 := db.(type) {
	case *gorm.DB:
		return gormdb.TSObjectHandler[T](db2)
	}
	return nil
}

func ListAnyObjects(db any, c echo.Context, tableName string, handleFuncs map[string]search.SearchDataHandleFunc) ([]map[string]any, int64, error) {
	switch db2 := db.(type) {
	case *gorm.DB:
		return gormdb.ListAnyObjects(db2, c, tableName, handleFuncs)
	}
	return nil, 0, errors.New("invalid db")
}
