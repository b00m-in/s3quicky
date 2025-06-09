/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	//"bytes"
	"encoding/binary"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

)

var(
	magicBytes                  = []byte("PAR1")
	magicEBytes                 = []byte("PARE")

)

// headCmd represents the head command
var headCmd = &cobra.Command{
	Use:   "head <object> <key>",
	Short: "Get only the headers of a key in an object store",
	Long: `Optionally peek at size of the metadata in the footer. For example:

s3quicky head -p overturemaps-us-west-2 release/2025-05-21.0/theme=buildings/type=building/part-00043-0df994ca-3323-4d7c-a374-68c653f78289-c000.zstd.parquet`,
	Run: func(cmd *cobra.Command, args []string) {

		bucket := args[0]
		key := args[1]
		//		filename := parseFilename(key)
		sess, err := session.NewSession(&aws.Config{Credentials: credentials.AnonymousCredentials, CredentialsChainVerboseErrors: aws.Bool(true), Region: aws.String("us-west-2")})
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}
		s3Client := s3.New(sess)

		if peek, _ := cmd.Flags().GetBool("peek"); !peek {
			head, err := getHeaders(s3Client, bucket, key)
			if err != nil {
				fmt.Printf("%v \n", err)//panic(err)
			}
			fmt.Printf("%v \n", head)
		} else {
			length, err := getContentLength(s3Client, bucket, key)
			if err != nil {
				fmt.Printf("%v \n", err)//panic(err)
			}
			var lasteight string
			if length != 0 {
				lasteight = fmt.Sprintf("bytes=%d-%d", length-8, length-1)
			}
			input := s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				//Range: aws.String("bytes=991368707-991368715"),
				Range: aws.String(lasteight),
			}
			output, err := s3Client.GetObject(&input)
			if err != nil {
				fmt.Printf("getobject %v \n", err)
			}
			defer output.Body.Close()
			//fmt.Printf("%s : %d \n", *output.AcceptRanges, *output.ContentLength)
			bs := make([]byte, 8)
			n, err := output.Body.Read(bs)
			if err != nil {
				fmt.Printf("body read %v %d \n", err, n)
			}
			//fmt.Printf("%s\n", bs[0:3])
			size := int64(binary.LittleEndian.Uint32(bs[:4]))
			fmt.Printf("%d %s\n", size, bs[4:8])
			//switch {
			//case bytes.Equal(bs[3:7], magicBytes): // non-encrypted metadata
				//	fmt.Println("parquet")
			//fmt.Printf("%v", bs)
			//}
		}
	},
}

func init() {
	rootCmd.AddCommand(headCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// headCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	headCmd.Flags().BoolP("peek", "p", false, "Peek at footer size")
}

func getHeaders(svc *s3.S3, bucket string, prefix string) (string, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(prefix),
	}
	resp, err := svc.HeadObject(params)
	if err != nil {
		return "error", err
	}
	return resp.String(), nil
}

func getContentLength(svc *s3.S3, bucket string, prefix string) (int64, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(prefix),
	}
	resp, err := svc.HeadObject(params)
	if err != nil {
		return 0, err
	}
	return *resp.ContentLength, nil
}

func getMetadata(svc *s3.S3, bucket string, prefix string) (map[string]*string, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(prefix),
	}
	resp, err := svc.HeadObject(params)
	if err != nil {
		return nil, err
	}
	return resp.Metadata, nil
}
