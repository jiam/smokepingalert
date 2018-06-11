package main

import (
	"fmt"
	"github.com/patrickmn/go-cache"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	alertFile = "/path/dead.letter"
)

type Config struct {
	Frequency  int
	Count      int
	Repeat     int
	Bccemail   string
	Hbaseemail string
}

func LoadConfig() Config {
	b, err := ioutil.ReadFile("./config.yaml")
	fmt.Println(string(b))
	if err != nil {
		log.Fatal(err)
	}
	c := Config{}
	err = yaml.Unmarshal(b, &c)
	if err != nil {
		log.Fatal(err)
	}
	return c

}

func SendEmail(subject, content, email string) (res string, err error) {
	resp, err := http.PostForm("url", url.Values{"appCode": {"monitor"}, "address": {email}, "content": {content}, "subject": {subject}})
	if err != nil {
		return "Email api wrong", err
	}
	fmt.Println("send email successful ", subject)
	return resp.Status, err
}

func main() {
	cf := LoadConfig()
	repeat := time.Duration(cf.Repeat)
	frequency := time.Duration(cf.Frequency)
	count := cf.Count
	bccemail := cf.Bccemail
	hbaseemail := cf.Hbaseemail

	f, err := os.Open(alertFile)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	fileInfo, _ := os.Stat(alertFile)
	fileSize := fileInfo.Size()
	f.Seek(fileSize, 0)

	messages := ""
	b := make([]byte, 1024)
	c := cache.New(repeat*time.Minute, repeat*time.Minute)

	for {
		n, err := f.Read(b)
		// 读取文件内容
		if n > 0 {
			messages += string(b[:n])
		}
		// 读到文件结束时，对已读取的报警进行处理
		if err == io.EOF {
			messageSlice := strings.Split(strings.TrimSpace(messages), "\n\n\n\n")
			m := make(map[string]int)
			bccbody := ""
			hbasebody := ""
			for i := 0; i < len(messageSlice); i++ {
				trimspace := strings.TrimSpace(messageSlice[i])
				lines := strings.Split(trimspace, "\n")
				if len(lines) == 16 {
					alert := lines[2]
					if val, ok := m[alert]; ok {
						m[alert] = val + 1
					} else {
						m[alert] = 1
					}
					if m[alert] == count {
						_, found := c.Get(alert)
						if !found {
							c.Set(alert, count, cache.DefaultExpiration)
							if strings.Index(alert, "hbase") != -1 {
								hbasebody = hbasebody + alert + "<p>"
							}
							if strings.Index(alert, "bcc") != -1 {
								bccbody = bccbody + alert + "<p>"
							}

						}
					}
				}

			}
			// 排除最后的元素，发送邮件
			if len(hbasebody) > 0 {
				subject := fmt.Sprintf("[Smokeping Alert ] Alert loss5p Warning %d times", len(messageSlice))
				res, err := SendEmail(subject, hbasebody, hbaseemail)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println(res)
				}
			}
			if len(bccbody) > 0 {
				subject := fmt.Sprintf("[Smokeping Alert ] Alert loss5p Warning %d times", len(messageSlice))
				res, err := SendEmail(subject, bccbody, bccemail)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println(res)
				}
			}

			//清空messages变量，间隔一定时间继续循环
			messages = ""
			time.Sleep(frequency * time.Second)
		}
		if err != nil {
			messages = ""
			fmt.Println(err)
			time.Sleep(frequency * time.Second)
		}
		time.Sleep(1 * time.Second)

	}

}
