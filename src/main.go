package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	// "io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/logdb"
)

type Conf struct {
	Ak string `json:"ak"`
	Sk string `json:"sk"`

	Size   int    `json:"size"`
	Repo   string `json:"repo"`
	Fields string `json:"fields"`
	Step   int64  `json:"step"`
}

var conf Conf

func SetupConfiguration(path string) error {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, &conf)
}

func getEnd(startPos, endTime int64) int64 {
	e1 := startPos + conf.Step*int64(60)
	if e1 < endTime {
		return e1
	}
	return endTime
}

func main() {
	var confPath string
	var duration int64
	var endTime int64
	var query string
	var outputPath string
	flag.StringVar(&confPath, "c", "search.conf", "conf.json path")
	flag.Int64Var(&duration, "d", 15, "time duration, minute")
	flag.StringVar(&query, "q", "*", "query string")
	flag.Int64Var(&endTime, "t", time.Now().Unix(), "end time")
	flag.StringVar(&outputPath, "o", "output-"+strconv.FormatInt(int64(os.Getegid()), 10)+".log", "output path")
	flag.Parse()

	err := SetupConfiguration(confPath)
	if err != nil {
		log.Fatalln("load config failed ", err)
		return
	}
	if conf.Size == 0 {
		conf.Size = 100
	}
	if conf.Step == 0 {
		conf.Step = 5
	}

	f, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Println(err)
	}
	writer := csv.NewWriter(f)
	start := endTime - duration*int64(60)
	startPos := start
	for {
		endPos := getEnd(startPos, endTime)
		Query(query, startPos, endPos, writer, startPos == start)
		startPos += conf.Step * 60
		if startPos >= endTime {
			break
		}
	}

	f.Close()
}

func newLogdbClient() logdb.LogdbAPI {
	logger := base.NewDefaultLogger()
	lconf := config.NewConfig().
		WithEndpoint("https://logdb.qiniu.com").
		WithAccessKeySecretKey(conf.Ak, conf.Sk).
		WithLogger(logger).
		WithLoggerLevel(base.LogDebug)
	client, err := logdb.New(lconf)
	if err != nil {
		log.Println(err)
		return nil
	}
	return client
}

func parseTitle(prefix string, data map[string]interface{}) (titles []string) {
	for key, value := range data {
		if v, ok := value.(map[string]interface{}); ok {
			titles = append(titles, parseTitle(prefix+key+".", v)...)
		} else {
			titles = append(titles, prefix+key)
		}
	}
	return
}

func parseValue(index map[string]int, prefix string, values []string, data map[string]interface{}) {
	for key, value := range data {
		if v, ok := value.(map[string]interface{}); ok {
			parseValue(index, prefix+key+".", values, v)
		} else {
			values[index[prefix+key]] = fmt.Sprintf("%v", value)
		}
	}
	return
}

func Query(q string, startTime int64, endTime int64, writer *csv.Writer, first bool) {
	var input logdb.QueryAnalysisLogInput
	input.RepoName = conf.Repo
	input.Query = q

	end := time.Unix(endTime, 0)
	input.Size = conf.Size

	input.Start = time.Unix(startTime, 0)
	input.End = end
	input.Fields = conf.Fields
	client := newLogdbClient()
	id, err := client.QueryAnalysisLogJob(&input)
	if err != nil {
		log.Println(err.Error())
		return
	}

	var data []map[string]interface{} = make([]map[string]interface{}, 0)
	partial := true
	log.Println(id)
	for partial == true {
		output, err := client.QueryAnalysisLog(id)
		partial = output.PartialSuccess

		if err != nil {
			log.Println(err.Error())
			return
		}
		for _, v := range output.Hits {
			data = append(data, v.Data)
		}
	}

	if len(data) == 0 {
		return
	}
	titles := parseTitle("", data[0])
	sort.Strings(titles)
	indexes := make(map[string]int)
	for k, v := range titles {
		indexes[v] = k
	}
	if first {
		writer.Write(titles)
	}
	for _, v := range data {
		values := make([]string, len(indexes))
		parseValue(indexes, "", values, v)
		writer.Write(values)
	}
}
