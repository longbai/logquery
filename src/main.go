package main

import (
	"encoding/json"
	"flag"
	// "fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/logdb"
)

type Conf struct {
	Ak string `json:"ak"`
	Sk string `json:"sk"`

	Size   int    `json:"size"`
	Fields string `json:"fields"`
	Repo   string `json:"repo"`
}

var conf Conf

func SetupConfiguration(path string) error {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, &conf)
}

func main() {
	var confPath string
	var duration int
	var endTime int64
	var query string
	var outputPath string
	flag.StringVar(&confPath, "c", "conf.json", "conf.json path")
	flag.IntVar(&duration, "d", 15, "time duration, minute")
	flag.StringVar(&query, "q", "*", "query string")
	flag.Int64Var(&endTime, "t", time.Now().Unix(), "end time")
	flag.StringVar(&outputPath, "o", "", "output path")
	flag.Parse()

	err := SetupConfiguration(confPath)
	if err != nil {
		log.Fatalln("load config failed ", err)
		return
	}

	Query(query, duration, endTime, outputPath)
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

func Query(q string, duration int, endTime int64, output string) {
	var input logdb.QueryAnalysisLogInput
	input.RepoName = conf.Repo
	input.Query = q

	end := time.Unix(endTime, 0)
	input.Size = conf.Size
	input.Start = end.Add(-time.Duration(duration) * time.Minute)
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
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Println(err.Error())
		return
	}
	if output != "" {
		ioutil.WriteFile(output, b, os.ModePerm)
		return
	} else {
		log.Println(string(b))
	}

}
