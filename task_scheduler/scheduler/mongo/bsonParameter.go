package mongo

import "go.mongodb.org/mongo-driver/bson"

type BSONParameter struct {
	params bson.M
}

func NewBSONParameter(params bson.M) *BSONParameter {
	return &BSONParameter{
		params: params,
	}
}

func (bp *BSONParameter) ToMap() (map[string]interface{}, error) {
	return map[string]interface{}(bp.params), nil
}