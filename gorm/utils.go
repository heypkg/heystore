package gormdb

import (
	"fmt"
	"net/http"

	"github.com/heypkg/store/search"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"gorm.io/gorm"
)

func appendToTotalParamsToDBWithHandlers(db *gorm.DB, c echo.Context, handleFuncs map[string]search.SearchDataHandleFunc) (*gorm.DB, error) {
	schema := cast.ToString(c.Get("schema"))

	q := c.QueryParam("q")
	q = q + " schema:" + schema
	search, err := search.ParseSearchString(q)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusBadRequest, errors.Wrap(err, "invalid query").Error())
	}
	out := search.SearchDB(db, handleFuncs)
	return out, nil
}

func appendToListParamsToDBWithHandlers(db *gorm.DB, c echo.Context, total int, handleFuncs map[string]search.SearchDataHandleFunc) (*gorm.DB, error) {
	schema := cast.ToString(c.Get("schema"))
	page := cast.ToInt(c.QueryParam("page"))
	pageSize := cast.ToInt(c.QueryParam("page_size"))

	q := c.QueryParam("q")
	q = q + " schema:" + schema
	s, err := search.ParseSearchString(q)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusBadRequest, errors.Wrap(err, "invalid query").Error())
	}
	out := s.SearchDB(db, handleFuncs)

	order := c.QueryParam("order_by")
	orders := search.ParseOrderByString(order)
	for _, v := range orders {
		if v.Desc {
			out = out.Order(v.Name + " DESC")
		} else {
			out = out.Order(v.Name)
		}
	}

	if pageSize > 0 {
		totalPage := (total + pageSize - 1) / pageSize
		if page > totalPage {
			page = totalPage
		}
		if page < 1 {
			page = 1
		}
		out = out.Offset((page - 1) * pageSize).Limit(pageSize)
	}
	return out, nil
}

func getTotalParamsToStringWithHandlers(c echo.Context, handleFuncs map[string]search.SearchDataHandleFunc) (string, []any, error) {
	schema := cast.ToString(c.Get("schema"))

	q := c.QueryParam("q")
	q = q + " schema:" + schema
	search, err := search.ParseSearchString(q)
	if err != nil {
		return "", nil, echo.NewHTTPError(http.StatusBadRequest, errors.Wrap(err, "invalid query").Error())
	}
	where, args, err := search.WhereString(handleFuncs)
	if err != nil {
		return "", nil, echo.NewHTTPError(http.StatusBadRequest, errors.Wrap(err, "invalid query").Error())
	}
	return where, args, nil
}

func getListParamsToStringWithHandlers(c echo.Context, total int, handleFuncs map[string]search.SearchDataHandleFunc) (string, []any, error) {
	schema := cast.ToString(c.Get("schema"))
	page := cast.ToInt(c.QueryParam("page"))
	pageSize := cast.ToInt(c.QueryParam("page_size"))

	q := c.QueryParam("q")
	q = q + " schema:" + schema
	search, err := search.ParseSearchString(q)
	if err != nil {
		return "", nil, echo.NewHTTPError(http.StatusBadRequest, errors.Wrap(err, "invalid query").Error())
	}
	where, args, err := search.WhereString(handleFuncs)
	if err != nil {
		return "", nil, echo.NewHTTPError(http.StatusBadRequest, errors.Wrap(err, "invalid query").Error())
	}

	if pageSize > 0 {
		totalPage := (total + pageSize - 1) / pageSize
		if page > totalPage {
			page = totalPage
		}
		if page < 1 {
			page = 1
		}
		where = where + fmt.Sprintf(" offset %v limit %v", (page-1)*pageSize, pageSize)
	}
	return where, args, nil
}
