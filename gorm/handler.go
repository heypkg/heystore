package gormdb

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/heypkg/store/jsontype"
	"github.com/heypkg/store/search"
	"github.com/heypkg/store/utils"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"gorm.io/gorm"
)

func ListObjects[T any](db *gorm.DB, c echo.Context, selectNames []string, handleFuncs map[string]search.SearchDataHandleFunc) ([]T, int64, error) {
	var err error
	var obj T

	db2 := db.Model(&obj)
	db2, err = appendToTotalParamsToDBWithHandlers(db2, c, handleFuncs)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if err := db2.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	db2 = db.Model(&obj)
	if len(selectNames) > 0 {
		db2 = db2.Select(selectNames)
	}
	db2, err = appendToListParamsToDBWithHandlers(db2, c, int(total), handleFuncs)
	if err != nil {
		return nil, 0, err
	}
	preloads := strings.Split(cast.ToString(c.Get("preload")), ",")
	for _, preload := range preloads {
		if preload != "" {
			db2.Preload(preload)
		}
	}
	var data []T
	if result := db2.Find(&data); result.Error != nil {
		return nil, 0, err
	}
	return data, total, nil
}

func ListDeletedObjects[T any](db *gorm.DB, c echo.Context, selectNames []string, handleFuncs map[string]search.SearchDataHandleFunc) ([]T, int64, error) {
	var err error
	var obj T
	db2 := db.Model(&obj).Unscoped().Where("deleted IS NOT NULL")
	db, err = appendToTotalParamsToDBWithHandlers(db2, c, handleFuncs)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	var data []T

	if err := db2.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return data, 0, nil
	}
	db2 = db.Model(&obj).Unscoped().Where("deleted IS NOT NULL")
	if len(selectNames) > 0 {
		db2 = db2.Select(selectNames)
	}

	db2, err = appendToListParamsToDBWithHandlers(db2, c, int(total), handleFuncs)
	if err != nil {
		return nil, 0, err
	}

	preloads := strings.Split(cast.ToString(c.Get("preload")), ",")
	for _, preload := range preloads {
		if preload != "" {
			db2.Preload(preload)
		}
	}

	if result := db2.Find(&data); result.Error != nil {
		return nil, 0, err
	}
	return data, total, nil
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

func ObjectHandler[T any](db *gorm.DB) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			var obj T
			key := utils.GetRawTypeName(obj)
			schema := cast.ToString(c.Get("schema"))
			id := cast.ToUint(c.Param("id"))

			db2 := db
			preloads := strings.Split(cast.ToString(c.Get("preload")), ",")
			for _, preload := range preloads {
				if preload != "" {
					db2.Preload(preload)
				}
			}

			result := db2.Where("schema = ? AND id = ?", schema, id).First(&obj)

			if result.Error != nil {
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					return echo.NewHTTPError(http.StatusNotFound, "not found")
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, result.Error)
				}
			} else {
				c.Set(key, &obj)
			}
			return next(c)
		}
	}
}

func DeletedObjectHandler[T any](db *gorm.DB) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			var obj T
			key := utils.GetRawTypeName(obj)
			schema := cast.ToString(c.Get("schema"))
			id := cast.ToUint(c.Param("id"))

			db2 := db.Unscoped()
			result := db2.Where("schema = ? AND id = ? AND deleted IS NOT NULL", schema, id).First(&obj)

			if result.Error != nil {
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					return echo.NewHTTPError(http.StatusNotFound, "not found")
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, result.Error)
				}
			} else {
				c.Set(key, &obj)
			}
			return next(c)
		}
	}
}

func TSObjectHandler[T any](db *gorm.DB) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			var obj T
			key := utils.GetRawTypeName(obj)
			schema := cast.ToString(c.Get("schema"))
			ts := cast.ToInt64(c.Param("ts"))

			db2 := db
			result := db2.Where("schema = ? AND time = ?", schema, jsontype.JSONTime(time.Unix(0, ts*int64(time.Microsecond)))).First(&obj)
			if result.Error != nil {
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					return echo.NewHTTPError(http.StatusNotFound, "not found")
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, result.Error)
				}
			} else {
				c.Set(key, &obj)
			}
			return next(c)
		}
	}
}

func ListAnyObjects(db *gorm.DB, c echo.Context, tableName string, handleFuncs map[string]search.SearchDataHandleFunc) ([]map[string]any, int64, error) {
	var err error
	db2 := db

	where, args, err := getTotalParamsToStringWithHandlers(c, handleFuncs)
	if err != nil {
		return nil, 0, err
	}

	var total int64
	records := []map[string]any{}

	q := fmt.Sprintf("select count(*) from %v where %v", tableName, where)
	if err := db2.Debug().Raw(q, args...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return records, 0, nil
	}

	where, args, err = getListParamsToStringWithHandlers(c, int(total), handleFuncs)
	if err != nil {
		return nil, 0, err
	}

	q = fmt.Sprintf("select * from %v where %v", tableName, where)
	rows, err := db2.Debug().Raw(q, args...).Rows()
	if err != nil {
		return nil, 0, errors.Wrap(err, "query")
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	values := make([]any, len(columns))
	for i := range values {
		values[i] = new(any)
	}
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			return nil, 0, errors.Wrap(err, "scan record")
		}
		record := map[string]any{}
		for i, column := range columns {
			record[column] = *values[i].(*any)
		}
		records = append(records, record)
	}
	return records, total, nil
}
