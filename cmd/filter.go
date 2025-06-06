/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"
	"strings"
	"strconv"
	
	"b00m.in/s3quicky/cmd/util"
	"github.com/spf13/cobra"
	"github.com/paulmach/orb"
)

// filterCmd represents the filter command
var filterCmd = &cobra.Command{
	Use:   "filter <filename>",
	Short: "Tool to filter parquet column groups by values",
	Long: `Apply filters to column groups in parquet files to check if a file has data of interest, for example:

s3quicky filter --column 4 --gt 3.2 -lt 6.6 part-0fd324452.parquet 
s3quicky filter --columns 4,5,6,7 --bbox -3.71,40.41,-3.69,40.43 part-0fd324452.parquet`,
	Args: util.Validate,
	Run: func(cmd *cobra.Command, args []string) {
		var bbox string
		var err error
		var bound orb.Bound
		if bbox, _ = cmd.Flags().GetString("bbox"); bbox != "" {
			bound, err = parseBbox(bbox)
			if err != nil {
				fmt.Printf("%v\n", err)
				return
			}
		}
		fmt.Println(bound)		
	},
}

func init() {
	rootCmd.AddCommand(filterCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// filterCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// filterCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	filterCmd.Flags().IntP("column", "c", 4, "the column number to filter on")
	filterCmd.Flags().Float32P("gt", "g", 3.0, "the lower bound")
	filterCmd.Flags().Float32P("lt", "l", 1.0, "the upper bound")
	filterCmd.Flags().StringP("bbox", "b", "-3.71,40.41,-3.69,40.43", "the min lat, min lng, max lat, max lng of the bounding box")
}

var emptyBound = orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{-1, -1}}
func parseBbox(bbox string) (orb.Bound, error){
	var minx, miny, maxx, maxy float64
	cs := strings.Split(bbox, ",")
	if len(cs)==4 {
		minx,_ = strconv.ParseFloat(cs[0], 64)
		miny,_ = strconv.ParseFloat(cs[1], 64)
		maxx,_ = strconv.ParseFloat(cs[2], 64)
		maxy,_ = strconv.ParseFloat(cs[3], 64)
	} else {
		return emptyBound, fmt.Errorf("found malformed bbox len %d",len(cs))
	}
	bound := orb.Bound{Min: orb.Point{minx,miny}, Max: orb.Point{maxx,maxy}}
	return bound, nil
}
