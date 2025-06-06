/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"

	"b00m.in/s3quicky/parquet"
	"github.com/spf13/cobra"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show <object> <key>",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		bucket := args[0]
		key := args[1]
		filename := parseFilename(key)
		sess, err := session.NewSession(&aws.Config{Credentials: credentials.AnonymousCredentials, CredentialsChainVerboseErrors: aws.Bool(true), Region: aws.String("us-west-2")})
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}
		s3Client := s3.New(sess)

		length, err := getFileSize(s3Client, bucket, key)
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}
		var lasteight string
		if length != 0 {
			lasteight = fmt.Sprintf("bytes=%d-%d", length-8, length-1)
		} else {
			fmt.Println("object key has no length")
			return
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

		bs := make([]byte, 8)
		n, err := output.Body.Read(bs)
		if err != nil {
			fmt.Printf("body read %v %d \n", err, n)
		}
		size := int64(binary.LittleEndian.Uint32(bs[:4]))
		downloader := s3manager.NewDownloader(sess)
		rangotxt := fmt.Sprintf("bytes-%d-%d", length-size-8, length)
		rango := fmt.Sprintf("bytes=%d-%d", length-size-8, length)
		params := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Range: aws.String(rango),
		}
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}

		temp, err := ioutil.TempFile(cwd, "getObjWithProgress-tmp-")
		if err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}
		tempfileName := temp.Name()

		if _, err := downloader.Download(temp, params); err != nil {
			fmt.Printf("Download failed! Deleting tempfile: %s", tempfileName)
			os.Remove(tempfileName)
			fmt.Printf("%v \n", err)//panic(err)
		}

		if err := temp.Close(); err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}

		filename = modifyFilename(filename, rangotxt)
		if err := os.Rename(temp.Name(), filename); err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}

		if err = parquet.Deserialize(filename, true); err != nil {
			fmt.Printf("%v \n", err)//panic(err)
		}

	},
}

func init() {
	rootCmd.AddCommand(showCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// showCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// showCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

