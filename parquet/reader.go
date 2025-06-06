package parquet


import (
	"fmt"
	"log"
	"os"

	//"github.com/apache/arrow-go/v18/arrow/memory"
	//"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	//"github.com/apache/arrow-go/v18/parquet/internal/encryption"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	//"golang.org/x/xerrors"

)


func Deserialize(filename string, memorymap bool) error {

	var config struct { NoMetadata bool
		OnlyMetadata bool
		PrintKeyValueMetadata bool
	} 
	rdr, err := file.OpenParquetFile(filename, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error opening parquet file: ", err)
		os.Exit(1)
	}

	fileMetadata := rdr.MetaData()

	if !config.NoMetadata {
		fmt.Println("File name:", filename)
		fmt.Println("Version:", fileMetadata.Version())
		fmt.Println("Created By:", fileMetadata.GetCreatedBy())
		fmt.Println("Num Rows:", rdr.NumRows())

		keyvaluemeta := fileMetadata.KeyValueMetadata()
		if config.PrintKeyValueMetadata && keyvaluemeta != nil {
			fmt.Println("Key Value File Metadata:", keyvaluemeta.Len(), "entries")
			keys := keyvaluemeta.Keys()
			values := keyvaluemeta.Values()
			for i := 0; i < keyvaluemeta.Len(); i++ {
				fmt.Printf("Key nr %d %s: %s\n", i, keys[i], values[i])
			}
		}

		fmt.Println("Number of RowGroups:", rdr.NumRowGroups())
		fmt.Println("Number of Real Columns:", fileMetadata.Schema.Root().NumFields())
		fmt.Println("Number of Columns:", fileMetadata.Schema.NumColumns())
	}

	selectedColumns := []int{}
	if len(selectedColumns) == 0 {
		for i := 0; i < fileMetadata.Schema.NumColumns(); i++ {
			selectedColumns = append(selectedColumns, i)
		}
	} else {
		for _, c := range selectedColumns {
			if c < 0 || c >= fileMetadata.Schema.NumColumns() {
				fmt.Fprintln(os.Stderr, "selected column is out of range")
				os.Exit(1)
			}
		}
	}

	if !config.NoMetadata {
		fmt.Println("Number of Selected Columns:", len(selectedColumns))
		for _, c := range selectedColumns {
			descr := fileMetadata.Schema.Column(c)
			fmt.Printf("Column %d: %s (%s", c, descr.Path(), descr.PhysicalType())
			if descr.ConvertedType() != schema.ConvertedTypes.None {
				fmt.Printf("/%s", descr.ConvertedType())
				if descr.ConvertedType() == schema.ConvertedTypes.Decimal {
					dec := descr.LogicalType().(schema.DecimalLogicalType)
					fmt.Printf("(%d,%d)", dec.Precision(), dec.Scale())
				}
			}
			fmt.Print(")\n")
		}
	}

	for r := 0; r < rdr.NumRowGroups(); r++ {
		if !config.NoMetadata {
			fmt.Println("--- Row Group:", r, " ---")
		}

		rgr := rdr.RowGroup(r)
		rowGroupMeta := rgr.MetaData()
		if !config.NoMetadata {
			fmt.Println("--- Total Bytes:", rowGroupMeta.TotalByteSize(), " ---")
			fmt.Println("--- Rows:", rgr.NumRows(), " ---")
		}

		for _, c := range selectedColumns {
			chunkMeta, err := rowGroupMeta.ColumnChunk(c)
			if err != nil {
				log.Fatal(err)
			}

			if !config.NoMetadata {
				fmt.Println("Column", c)
				if set, _ := chunkMeta.StatsSet(); set {
					stats, err := chunkMeta.Statistics()
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf(" Values: %d", chunkMeta.NumValues())
					if stats.HasMinMax() {
						fmt.Printf(", Min: %v, Max: %v",
							metadata.GetStatValue(stats.Type(), stats.EncodeMin()),
							metadata.GetStatValue(stats.Type(), stats.EncodeMax()))
					}
					if stats.HasNullCount() {
						fmt.Printf(", Null Values: %d", stats.NullCount())
					}
					if stats.HasDistinctCount() {
						fmt.Printf(", Distinct Values: %d", stats.DistinctCount())
					}
					fmt.Println()
				} else {
					fmt.Println(" Values:", chunkMeta.NumValues(), "Statistics Not Set")
				}

				fmt.Print(" Compression: ", chunkMeta.Compression())
				fmt.Print(", Encodings:")
				for _, enc := range chunkMeta.Encodings() {
					fmt.Print(" ", enc)
				}
				fmt.Println()

				fmt.Print(" Uncompressed Size: ", chunkMeta.TotalUncompressedSize())
				fmt.Println(", Compressed Size:", chunkMeta.TotalCompressedSize())
			}
		}

		if config.OnlyMetadata {
			continue
		}

		if !config.NoMetadata {
			fmt.Println("--- Values ---")
		}

		/*switch {
		case config.JSON:
			fmt.Fprint(dataOut, "[")

			scanners := make([]*Dumper, len(selectedColumns))
			fields := make([]string, len(selectedColumns))
			for idx, c := range selectedColumns {
				col, err := rgr.Column(c)
				if err != nil {
					log.Fatalf("unable to fetch column=%d err=%s", c, err)
				}
				scanners[idx] = createDumper(col)
				fields[idx] = col.Descriptor().Path()
			}

			var line string
			for {
				if line == "" {
					line = "\n  {"
				} else {
					line = ",\n  {"
				}

				data := false
				first := true
				for idx, s := range scanners {
					if val, ok := s.Next(); ok {
						if !data {
							fmt.Fprint(dataOut, line)
						}
						data = true
						if val == nil {
							continue
						}
						if !first {
							fmt.Fprint(dataOut, ",")
						}
						first = false
						switch val.(type) {
						case bool, int32, int64, float32, float64:
						default:
							val = s.FormatValue(val, 0)
						}
						jsonVal, err := json.Marshal(val)
						if err != nil {
							fmt.Fprintf(os.Stderr, "error: marshalling json for %+v, %s\n", val, err)
							os.Exit(1)
						}
						fmt.Fprintf(dataOut, "\n    %q: %s", fields[idx], jsonVal)
					}
				}
				if !data {
					break
				}
				fmt.Fprint(dataOut, "\n  }")
			}

			fmt.Fprintln(dataOut, "\n]")
		case config.CSV:
			scanners := make([]*Dumper, len(selectedColumns))
			for idx, c := range selectedColumns {
				if idx > 0 {
					fmt.Fprint(dataOut, ",")
				}
				col, err := rgr.Column(c)
				if err != nil {
					log.Fatalf("unable to fetch col=%d err=%s", c, err)
				}
				scanners[idx] = createDumper(col)
				fmt.Fprintf(dataOut, "%q", col.Descriptor().Path())
			}
			fmt.Fprintln(dataOut)

			var line string
			for {
				data := false
				for idx, s := range scanners {
					if idx > 0 {
						if data {
							fmt.Fprint(dataOut, ",")
						} else {
							line += ","
						}
					}
					if val, ok := s.Next(); ok {
						if !data {
							fmt.Fprint(dataOut, line)
						}
						data = true
						if val == nil {
							fmt.Fprint(dataOut, "")
							continue
						}
						switch val.(type) {
						case bool, int32, int64, parquet.Int96, float32, float64:
							fmt.Fprintf(dataOut, "%v", val)
						default:
							fmt.Fprintf(dataOut, "%q", s.FormatValue(val, 0))
						}
					} else {
						if data {
							fmt.Fprint(dataOut, ",")
						} else {
							line += ","
						}
					}
				}
				if !data {
					break
				}
				fmt.Fprintln(dataOut)
				line = ""
			}
			fmt.Fprintln(dataOut)
		default:
			const colwidth = 18

			scanners := make([]*Dumper, len(selectedColumns))
			for idx, c := range selectedColumns {
				col, err := rgr.Column(c)
				if err != nil {
					log.Fatalf("unable to fetch column=%d err=%s", c, err)
				}
				scanners[idx] = createDumper(col)
				fmt.Fprintf(dataOut, fmt.Sprintf("%%-%ds|", colwidth), col.Descriptor().Name())
			}
			fmt.Fprintln(dataOut)

			var line string
			for {
				data := false
				for _, s := range scanners {
					if val, ok := s.Next(); ok {
						if !data {
							fmt.Fprint(dataOut, line)
						}
						fmt.Fprint(dataOut, s.FormatValue(val, colwidth), "|")
						data = true
					} else {
						if data {
							fmt.Fprintf(dataOut, fmt.Sprintf("%%-%ds|", colwidth), "")
						} else {
							line += fmt.Sprintf(fmt.Sprintf("%%-%ds|", colwidth), "")
						}
					}
				}
				if !data {
					break
				}
				fmt.Fprintln(dataOut)
				line = ""
			}
			fmt.Fprintln(dataOut)
		}*/
	}
	return nil
}
