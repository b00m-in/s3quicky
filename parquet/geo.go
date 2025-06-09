package parquet

import (

)

type Geo struct {
	Version       string `json:"version"`
	PrimaryColumn string `json:"primary_column"`
	Columns       struct {
		Geometry struct {
			Encoding      string    `json:"encoding"`
			GeometryTypes []string  `json:"geometry_types"`
			Bbox          []float64 `json:"bbox"`
			Covering      struct {
				Bbox struct {
					Xmin []string `json:"xmin"`
					Ymin []string `json:"ymin"`
					Xmax []string `json:"xmax"`
					Ymax []string `json:"ymax"`
				} `json:"bbox"`
			} `json:"covering"`
		} `json:"geometry"`
	} `json:"columns"`
}
