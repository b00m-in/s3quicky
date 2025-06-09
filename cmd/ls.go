/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"

	"b00m.in/s3quicky/cmd/util"
	"github.com/spf13/cobra"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/paulmach/orb"
)

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls [options] <s3bucket>",
	//Use:   fmt.Sprintf("ls [options] <%s>", util.ToKebabCase(ResourceNameSingular)),
	Short: "List the keys in an S3 bucket",
	Long: `List the keys in an S3 bucket with optional prefix/bbox to limit list (max 1000). For example:

 s3quicky ls --bbox -3.71,40.40,-3.69,40.44 --prefix release/2025-05-21.0/theme=buildings/type=building overturemapsus-west-2

or provide empty bounds (xmin > xmax) to ignore default bbox.

 s3quicky ls --bbox -3.71,40.40,-3.79,40.44 --prefix release/2025-05-21.0/theme=buildings/type=building overturemapsus-west-2`,
	Args: util.Validate,
	Run: func(cmd *cobra.Command, args []string) {
		//fmt.Println("ls called")

		var prefix string
		if prefix, _ = cmd.Flags().GetString("prefix"); prefix == "" {
			prefix = "release/2025-05-21"
		}

		var bbox string
		var err error
		var bounds orb.Bound
		if bbox, _ = cmd.Flags().GetString("bbox"); bbox != "" {
			bounds, err = parseBbox(bbox)
			if err != nil {
				fmt.Printf("%v\n", err)
			}
		}
		var limit bool
		if !bounds.IsEmpty() {
			limit = true
		}
		
		// Initialize a session in us-west-2 that the SDK will use to load
		// credentials from the shared credentials file ~/.aws/credentials.
		sess, err := session.NewSession(&aws.Config{
			Credentials: credentials.AnonymousCredentials,
			CredentialsChainVerboseErrors: aws.Bool(true),
			Endpoint: aws.String("s3.us-west-2.amazonaws.com"),
			//Endpoint: aws.String("overturemaps-us-west-2.s3.us-west-2.amazonaws.com/2025-04-23.0"),
			//Endpoint: aws.String("s3.amazonaws.com/overturemaps"),//-us-west-2"),
			Region: aws.String("us-west-2"),
			//S3ForcePathStyle: aws.Bool(true)
		},
		)

		// Create S3 service client
		svc := s3.New(sess)

		// Get the list of items
		bucket := args[0]
		resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix) /*"bridgefiles/2025-04-23")*/})
		if err != nil {
			fmt.Errorf("Unable to list items in bucket %q, %v", args[0], err)
		}

		fmt.Println("Found", len(resp.Contents), "items in bucket", args[0])
		if limit {
			for _, item := range resp.Contents {
				key := *item.Key
				length := *item.Size
				footersize := getFooterMetadataSize(svc, bucket, key, length)
				fmt.Printf("%s : %d\n", *item.Key, footersize)
				var filename string
				if filename, err = downloadMetadata(sess, bucket, key, length, footersize); err != nil {
					fmt.Println(err)
					return
				}
				if ok := intersects(filename, bbox); ok {
					fmt.Println(filename)
				}
				
			}
		} else {
			for _, item := range resp.Contents {
				fmt.Println("Name:         ", *item.Key)
				fmt.Println("Last modified:", *item.LastModified)
				fmt.Println("Size:         ", *item.Size)
				fmt.Println("Storage class:", *item.StorageClass)
				//fmt.Println("Storage class:", *item.StorageClass)
				fmt.Println("")
			}
		}
		
		fmt.Println("")
	},
}

func init() {
	rootCmd.AddCommand(lsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// lsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	//lsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	lsCmd.Flags().StringP("prefix", "p", "release/2025-05-21", "Provide prefix to key")
	lsCmd.Flags().StringP("bbox", "b", "-3.72,40.40,-3.68,40.44", "only include the keys where the bbox metadata intersects with the min lat, min lng, max lat, max lng of the provided bounding box")

}
