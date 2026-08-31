package requestvalidator

import (
	"net/http"

	"github.com/go-playground/validator/v10"
)

type Validator struct {
	validate *validator.Validate
}

func New() *Validator {
	return &Validator{
		validate: validator.New(validator.WithRequiredStructEnabled()),
	}
}

func (v *Validator) Validate(_ *http.Request, data any) error {
	return v.validate.Struct(data)
}
