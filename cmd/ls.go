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
)

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls [options] <s3bucket>",
	//Use:   fmt.Sprintf("ls [options] <%s>", util.ToKebabCase(ResourceNameSingular)),
	Short: "List the keys in an S3 bucket",
	Long: `List the keys in an S3 bucket with the bucket name as . For example:
 s3quicky ls overturemapsus-west-2`,
	Args: util.Validate,
	Run: func(cmd *cobra.Command, args []string) {
		//fmt.Println("ls called")

		var prefix string
		if prefix, _ = cmd.Flags().GetString("prefix"); prefix == "" {
			prefix = "release/2025-05-21"
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
		resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(args[0]), Prefix: aws.String(prefix) /*"bridgefiles/2025-04-23")*/})
		if err != nil {
			fmt.Errorf("Unable to list items in bucket %q, %v", args[0], err)
		}

		for _, item := range resp.Contents {
			fmt.Println("Name:         ", *item.Key)
			fmt.Println("Last modified:", *item.LastModified)
			fmt.Println("Size:         ", *item.Size)
			fmt.Println("Storage class:", *item.StorageClass)
			//fmt.Println("Storage class:", *item.StorageClass)
			fmt.Println("")
		}

		fmt.Println("Found", len(resp.Contents), "items in bucket", args[0])
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
}
