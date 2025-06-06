/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	
	"b00m.in/s3quicky/ui"
	"github.com/spf13/cobra"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get <bucket> <key>",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		//fmt.Println("get called")
		bucket := args[0]
		key := args[1]
		filename := parseFilename(key)
		sess, err := session.NewSession(&aws.Config{Credentials: credentials.AnonymousCredentials, CredentialsChainVerboseErrors: aws.Bool(true), Region: aws.String("us-west-2")})
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}
		s3Client := s3.New(sess)
		downloader := s3manager.NewDownloader(sess)
		size, err := getFileSize(s3Client, bucket, key)
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}

		fmt.Printf("Starting download, size: %d \n", ui.ByteCountDecimal(size))
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}

		temp, err := ioutil.TempFile(cwd, "getObjWithProgress-tmp-")
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}
		tempfileName := temp.Name()

		var start, end int
		var full bool
		start, _ = cmd.Flags().GetInt("start")
		if end, _ = cmd.Flags().GetInt("end"); end == -1 {
			full = true
		}

		rango := fmt.Sprintf("bytes=%d-%d", start, end)
		rangotxt := fmt.Sprintf("bytes-%d-%d", start, end)
		writer := &ui.ProgressWriter{Writer: temp, Size: size, Written: 0}
		params := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			//Range: aws.String(rango),
		}
		if !full {
			params.Range = aws.String(rango)
			filename = modifyFilename(filename, rangotxt)
		}

		if _, err := downloader.Download(writer, params); err != nil {
			fmt.Printf("Download failed! Deleting tempfile: %s", tempfileName)
			os.Remove(tempfileName)
			fmt.Printf("%v \n", err)//panic(err)
		}

		if err := temp.Close(); err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}

		if err := os.Rename(temp.Name(), filename); err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}

		fmt.Println()
		fmt.Printf("File downloaded! Available at: %s \n", filename)
	},
}

func init() {
	rootCmd.AddCommand(getCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// getCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// getCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	getCmd.Flags().IntP("start", "s", 991069492, "start of range")
	getCmd.Flags().IntP("end", "e", 991368715, "end of range (set to -1 for full download)")
	getCmd.MarkFlagsRequiredTogether("start", "end")
}

func getFileSize(svc *s3.S3, bucket string, prefix string) (filesize int64, error error) {
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

func parseFilename(keyString string) (filename string) {
	ss := strings.Split(keyString, "/")
	s := ss[len(ss)-1]
	return s
}

func modifyFilename(filename, add string) string {
	before, after, found := strings.Cut(filename, ".")
	if found {
		s := before + "-" + add + "." + after
		return s
	} else {
		return filename + "-" + add
	}
}
